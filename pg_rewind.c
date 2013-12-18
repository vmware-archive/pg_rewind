/*-------------------------------------------------------------------------
 *
 * pg_rewind.c
 *	  Synchronizes an old master server to a new timeline
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pg_rewind.h"
#include "fetch.h"
#include "filemap.h"

#include "access/timeline.h"
#include "access/xlog_internal.h"
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "storage/bufpage.h"

#include "getopt_long.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <unistd.h>

static void usage(const char *progname);

static void createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli,
				  XLogRecPtr checkpointloc);

static void digestControlFile(ControlFileData *ControlFile, char *source, size_t size);
static void sanityChecks(void);
static void findCommonAncestorTimeline(XLogRecPtr *recptr, TimeLineID *tli);

static ControlFileData ControlFile_target;
static ControlFileData ControlFile_source;

const char *progname;

char *datadir_target = NULL;
char *datadir_source = NULL;
char *connstr_source = NULL;

int verbose;
int dry_run;

static void
usage(const char *progname)
{
	printf("%s resynchronizes a cluster with another copy of the cluster.\n\n", progname);
	printf("Usage:\n  %s [OPTION]...\n\n", progname);
	printf("Options:\n");
	printf("  -D, --target-pgdata=DIRECTORY\n");
	printf("                 existing data directory to modify\n");
	printf("  --source-pgdata=DIRECTORY\n");
	printf("                 source data directory to sync with\n");
	printf("  --source-server=CONNSTR\n");
	printf("                 source server to sync with\n");
	printf("  -v             write a lot of progress messages\n");
	printf("  -n, --dry-run  stop before modifying anything\n");
	printf("  -V, --version  output version information, then exit\n");
	printf("  -?, --help     show this help, then exit\n");
	printf("\n");
	printf("Report bugs to https://github.com/vmware/pg_rewind.\n");
}


int
main(int argc, char **argv)
{
	static struct option long_options[] = {
		{"help", no_argument, NULL, '?'},
		{"target-pgdata", required_argument, NULL, 'D'},
		{"source-pgdata", required_argument, NULL, 1},
		{"source-server", required_argument, NULL, 2},
		{"version", no_argument, NULL, 'V'},
		{"dry-run", no_argument, NULL, 'n'},
		{"verbose", no_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};
	int			option_index;
	int			c;
	XLogRecPtr	divergerec;
	TimeLineID	lastcommontli;
	XLogRecPtr	chkptrec;
	TimeLineID	chkpttli;
	XLogRecPtr	chkptredo;
	size_t		size;
	char	   *buffer;
	bool		rewind_needed;

	progname = get_progname(argv[0]);

	/* Set default parameter values */
	verbose = 0;
	dry_run = 0;

	/* Process command-line arguments */
	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(0);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("pg_rewind " PG_REWIND_VERSION);
			exit(0);
		}
	}

	while ((c = getopt_long(argc, argv, "D:vn", long_options, &option_index)) != -1)
	{
		switch (c)
		{
			case '?':
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(1);
			case ':':
				exit(1);
			case 'v':
				verbose = 1;
				break;
			case 'n':
				dry_run = 1;
				break;

			case 'D':	/* -D or --target-pgdata */
				datadir_target = pg_strdup(optarg);
				break;

			case 1:		/* --source-pgdata */
				datadir_source = pg_strdup(optarg);
				break;
			case 2:		/* --source-server */
				connstr_source = pg_strdup(optarg);
				break;
		}
	}

	/* No source given? Show usage */
	if (datadir_source == NULL && connstr_source == NULL)
	{
		fprintf(stderr, "%s: no source specified\n", progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (datadir_target == NULL)
	{
		fprintf(stderr, "%s: no target data directory specified\n", progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	if (argc != optind)
	{
		fprintf(stderr, "%s: invalid arguments\n", progname);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
		exit(1);
	}

	/*
	 * Connect to remote server
	 */
	if (connstr_source)
		libpqConnect(connstr_source);

	/*
	 * Ok, we have all the options and we're ready to start. Read in all the
	 * information we need from both clusters.
	 */
	buffer = slurpFile(datadir_target, "global/pg_control", &size);
	digestControlFile(&ControlFile_target, buffer, size);
	pg_free(buffer);

	buffer = fetchFile("global/pg_control", &size);
	digestControlFile(&ControlFile_source, buffer, size);
	pg_free(buffer);

	sanityChecks();

	/*
	 * If both clusters are already on the same timeline, there's nothing
	 * to do.
	 */
	if (ControlFile_target.checkPointCopy.ThisTimeLineID == ControlFile_source.checkPointCopy.ThisTimeLineID)
	{
		fprintf(stderr, "source and target cluster are both on the same timeline.\n");
		exit(1);
	}

	findCommonAncestorTimeline(&divergerec, &lastcommontli);
	printf("The servers diverged at WAL position %X/%X on timeline %u.\n",
		   (uint32) (divergerec >> 32), (uint32) divergerec, lastcommontli);

	/*
	 * Check for the possibility that the target is in fact a direct ancestor
	 * of the source. In that case, there is no divergent history in the
	 * target that needs rewinding.
	 */
	if (ControlFile_target.checkPoint >= divergerec)
	{
		rewind_needed = true;
	}
	else
	{
		XLogRecPtr chkptendrec;

		/* Read the checkpoint record on the target to see where it ends. */
		chkptendrec = readOneRecord(datadir_target,
									ControlFile_target.checkPoint,
									ControlFile_target.checkPointCopy.ThisTimeLineID);

		/*
		 * If the histories diverged exactly at the end of the shutdown
		 * checkpoint record on the target, there are no WAL records in the
		 * target that don't belong in the source's history, and no rewind is
		 * needed.
		 */
		if (chkptendrec == divergerec)
			rewind_needed = false;
		else
			rewind_needed = true;
	}

	if (!rewind_needed)
	{
		printf("No rewind required.\n");
		exit(0);
	}
	findLastCheckpoint(datadir_target, divergerec, lastcommontli,
					   &chkptrec, &chkpttli, &chkptredo);
	printf("Rewinding from Last common checkpoint at %X/%X on timeline %u\n",
		   (uint32) (chkptrec >> 32), (uint32) chkptrec,
		   chkpttli);

	(void) filemap_create();
	fetchRemoteFileList();
	traverse_datadir(datadir_target, &process_local_file);

	/*
	 * Read the target WAL from last checkpoint before the point of fork,
	 * to extract all the pages that were modified on the target cluster
	 * after the fork.
	 */
	extractPageMap(datadir_target, chkptrec, lastcommontli);

	/* XXX: this is probably too verbose even in verbose mode */
	if (verbose)
		print_filemap();

	/* Ok, we're ready to start copying things over. */
	executeFileMap();

	createBackupLabel(chkptredo, chkpttli, chkptrec);

	printf("Done!\n");

	return 0;
}

static void
sanityChecks(void)
{
	/* Check that there's no backup_label in either cluster */
	/* Check system_id match */
	if (ControlFile_target.system_identifier != ControlFile_source.system_identifier)
	{
		fprintf(stderr, "source and target clusters are from different systems\n");
		exit(1);
	}
	/* check version */
	if (ControlFile_target.pg_control_version != PG_CONTROL_VERSION ||
		ControlFile_source.pg_control_version != PG_CONTROL_VERSION ||
		ControlFile_target.catalog_version_no != CATALOG_VERSION_NO ||
		ControlFile_source.catalog_version_no != CATALOG_VERSION_NO)
	{
		fprintf(stderr, "clusters are not compatible with this version of pg_rewind\n");
		exit(1);
	}

	/*
	 * Target cluster need to use checksums or hint bit wal-logging, this to
	 * prevent from data corruption that could occur because of hint bits.
	 */
	if (ControlFile_target.data_checksum_version != PG_DATA_CHECKSUM_VERSION &&
		!ControlFile_target.wal_log_hintbits)
	{
		fprintf(stderr, "target master need to use either data checksums or \"wal_log_hintbits = on\".\n");
		exit(1);
	}

	/*
	 * Target cluster better not be running. This doesn't guard against someone
	 * starting the cluster concurrently. Also, this is probably more strict
	 * than necessary; it's OK if the master was not shut down cleanly, as
	 * long as it isn't running at the moment.
	 */
	if (ControlFile_target.state != DB_SHUTDOWNED)
	{
		fprintf(stderr, "target master must be shut down cleanly.\n");
		exit(1);
	}
}

/*
 * Determine the TLI of the last common timeline in the histories of the two
 * clusters. *tli is set to the last common timeline, and *recptr is set to
 * the position where the histories diverged (ie. the first WAL record that's
 * not the same in both clusters).
 *
 * Control files of both clusters must be read into ControlFile_target/source
 * before calling this.
 */
static void
findCommonAncestorTimeline(XLogRecPtr *recptr, TimeLineID *tli)
{
	TimeLineID	targettli;
	TimeLineHistoryEntry *sourceHistory;
	int			nentries;
	int			i;
	TimeLineID	sourcetli;

	targettli = ControlFile_target.checkPointCopy.ThisTimeLineID;
	sourcetli = ControlFile_source.checkPointCopy.ThisTimeLineID;

	/* Timeline 1 does not have a history file, so no need to check */
	if (sourcetli == 1)
	{
		sourceHistory = (TimeLineHistoryEntry *) pg_malloc(sizeof(TimeLineHistoryEntry));
		sourceHistory->tli = sourcetli;
		sourceHistory->begin = sourceHistory->end = InvalidXLogRecPtr;
		nentries = 1;
	}
	else
	{
		char		path[MAXPGPATH];
		char	   *histfile;

		TLHistoryFilePath(path, sourcetli);
		histfile = fetchFile(path, NULL);

		sourceHistory = rewind_parseTimeLineHistory(histfile,
													ControlFile_source.checkPointCopy.ThisTimeLineID,
													&nentries);
		pg_free(histfile);
	}

	/*
	 * Trace the history backwards, until we hit the target timeline.
	 *
	 * TODO: This assumes that there are no timeline switches on the target
	 * cluster after the fork.
	 */
	for (i = nentries - 1; i >= 0; i--)
	{
		TimeLineHistoryEntry *entry = &sourceHistory[i];
		if (entry->tli == targettli)
		{
			/* found it */
			*recptr = entry->end;
			*tli = entry->tli;

			free(sourceHistory);
			return;
		}
	}

	fprintf(stderr, "could not find common ancestor of the source and target cluster's timelines\n");
	exit(1);
}


/*
 * Create a backup_label file that forces recovery to begin at the last common
 * checkpoint.
 */
static void
createBackupLabel(XLogRecPtr startpoint, TimeLineID starttli, XLogRecPtr checkpointloc)
{
	XLogSegNo	startsegno;
	char		BackupLabelFilePath[MAXPGPATH];
	FILE	   *fp;
	time_t		stamp_time;
	char		strfbuf[128];
	char		xlogfilename[MAXFNAMELEN];
	struct tm  *tmp;

	if (dry_run)
		return;

	XLByteToSeg(startpoint, startsegno);
	XLogFileName(xlogfilename, starttli, startsegno);

	/*
	 * TODO: move old file out of the way, if any. And perhaps create the
	 * file with temporary name first and rename in place after it's done.
	 */
	snprintf(BackupLabelFilePath, MAXPGPATH,
			 "%s/backup_label" /* BACKUP_LABEL_FILE */, datadir_target);

	/*
	 * Construct backup label file
	 */

	fp = fopen(BackupLabelFilePath, "wb");

	stamp_time = time(NULL);
	tmp = localtime(&stamp_time);
	strftime(strfbuf, sizeof(strfbuf), "%Y-%m-%d %H:%M:%S %Z", tmp);
	fprintf(fp, "START WAL LOCATION: %X/%X (file %s)\n",
			(uint32) (startpoint >> 32), (uint32) startpoint, xlogfilename);
	fprintf(fp, "CHECKPOINT LOCATION: %X/%X\n",
			(uint32) (checkpointloc >> 32), (uint32) checkpointloc);
	fprintf(fp, "BACKUP METHOD: rewound with pg_rewind\n");
	fprintf(fp, "BACKUP FROM: master\n");
	fprintf(fp, "START TIME: %s\n", strfbuf);
	/* omit LABEL: line */

	if (fclose(fp) != 0)
	{
		fprintf(stderr, _("could not write backup label file \"%s\": %s\n"),
				BackupLabelFilePath, strerror(errno));
		exit(2);
	}
}


/*
 * Verify control file contents in the buffer src, and copy it to *ControlFile.
 */
static void
digestControlFile(ControlFileData *ControlFile, char *src, size_t size)
{
	if (size != PG_CONTROL_SIZE)
	{
		fprintf(stderr, "unexpected control file size %d, expected %d\n",
				(int) size, PG_CONTROL_SIZE);
		exit(1);
	}
	memcpy(ControlFile, src, sizeof(ControlFileData));

	/* TODO: check crc */
}
