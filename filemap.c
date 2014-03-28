/*
 * A data structure for keeping track of files that have changed.
 *
 * Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 */

#include "postgres_fe.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <regex.h>

#include "datapagemap.h"
#include "filemap.h"
#include "util.h"
#include "pg_rewind.h"
#include "storage/fd.h"

filemap_t *filemap = NULL;

static bool isRelDataFile(const char *path);
static int path_cmp(const void *a, const void *b);
static int final_filemap_cmp(const void *a, const void *b);
static void filemap_list_to_array(void);


/*****
 * Public functions
 */

/*
 * Create a new file map.
 */
filemap_t *
filemap_create(void)
{
	filemap_t *map = pg_malloc(sizeof(filemap_t));
	map->first = map->last = NULL;
	map->nlist = 0;
	map->array = NULL;
	map->narray = 0;

	Assert(filemap == NULL);
	filemap = map;

	return map;
}

static bool
endswith(const char *haystack, const char *needle)
{
	int needlelen = strlen(needle);
	int haystacklen = strlen(haystack);

	if (haystacklen < needlelen)
		return false;

	return strcmp(&haystack[haystacklen - needlelen], needle) == 0;
}

/*
 * Callback for processing remote file list.
 */
void
process_remote_file(const char *path, file_type_t type, size_t newsize,
					const char *link_target)
{
	bool		exists;
	char		localpath[MAXPGPATH];
	struct stat statbuf;
	filemap_t  *map = filemap;
	file_action_t action = FILE_ACTION_NONE;
	size_t		oldsize = 0;
	file_entry_t *entry;

	Assert(map->array == NULL);

	/*
	 * Completely ignore some special files in source and destination.
	 */
	if (strcmp(path, "postmaster.pid") == 0 ||
		strcmp(path, "postmaster.opts") == 0)
		return;

	/*
	 * Skip temporary files, .../pgsql_tmp/... and .../pgsql_tmp.* in source.
	 * This has the effect that all temporary files in the destination will
	 * be removed.
	 */
	if (strstr(path, "/" PG_TEMP_FILE_PREFIX) != NULL)
		return;
	if (strstr(path, "/" PG_TEMP_FILES_DIR "/") != NULL)
		return;

	/*
	 * sanity check: a filename that looks like a data file better be a
	 * regular file
	 */
	if (type != FILE_TYPE_REGULAR && isRelDataFile(path))
	{
		fprintf(stderr, "data file in source \"%s\" is a directory.\n", path);
		exit(1);
	}

	snprintf(localpath, sizeof(localpath), "%s/%s", datadir_target, path);

	/* Does the corresponding local file exist? */
	if (lstat(localpath, &statbuf) < 0)
	{
		/* does not exist */
		if (errno != ENOENT)
		{
			fprintf(stderr, "could not stat file \"%s\": %s",
					localpath, strerror(errno));
			exit(1);
		}

		exists = false;
	}
	else
		exists = true;

	switch (type)
	{
		case FILE_TYPE_DIRECTORY:
			if (exists && !S_ISDIR(statbuf.st_mode))
			{
				/* it's a directory in target, but not in source. Strange.. */
				fprintf(stderr, "\"%s\" is not a directory.\n", localpath);
				exit(1);
			}

			if (!exists)
				action = FILE_ACTION_CREATE;
			else
				action = FILE_ACTION_NONE;
			oldsize = 0;
			break;

		case FILE_TYPE_SYMLINK:
			if (exists && !S_ISLNK(statbuf.st_mode))
			{
				/* it's a symbolic link in target, but not in source. Strange.. */
				fprintf(stderr, "\"%s\" is not a symbolic link.\n", localpath);
				exit(1);
			}

			if (!exists)
				action = FILE_ACTION_CREATE;
			else
				action = FILE_ACTION_NONE;
			oldsize = 0;
			break;

		case FILE_TYPE_REGULAR:
			if (exists && !S_ISREG(statbuf.st_mode))
			{
				fprintf(stderr, "\"%s\" is not a regular file.\n", localpath);
				exit(1);
			}

			if (!exists || !isRelDataFile(path))
			{
				/*
				 * File exists in source, but not in target. Or it's a non-data
				 * file that we have no special processing for. Copy it in toto.
				 *
				 * An exception: PG_VERSIONs should be identical, but avoid
				 * overwriting it for paranoia.
				 */
				if (endswith(path, "PG_VERSION"))
				{
					action = FILE_ACTION_NONE;
					oldsize = statbuf.st_size;
				}
				else
				{
					action = FILE_ACTION_COPY;
					oldsize = 0;
				}
			}
			else
			{
				/*
				 * It's a data file that exists in both.
				 *
				 * If it's larger in target, we can truncate it. There will
				 * also be a WAL record of the truncation in the source system,
				 * so WAL replay would eventually truncate the target too, but
				 * we might as well do it now.
				 *
				 * If it's smaller in the target, it means that it has been
				 * truncated in the target, or enlarged in the source, or both.
				 * If it was truncated locally, we need to copy the missing
				 * tail from the remote system. If it was enlarged in the
				 * remote system, there will be WAL records in the remote
				 * system for the new blocks, so we wouldn't need to copy them
				 * here. But we don't know which scenario we're dealing with,
				 * and there's no harm in copying the missing blocks now, so do
				 * it now.
				 *
				 * If it's the same size, do nothing here. Any locally modified
				 * blocks will be copied based on parsing the local WAL, and
				 * any remotely modified blocks will be updated after
				 * rewinding, when the remote WAL is replayed.
				 */
				oldsize = statbuf.st_size;
				if (oldsize < newsize)
					action = FILE_ACTION_COPY_TAIL;
				else if (oldsize > newsize)
					action = FILE_ACTION_TRUNCATE;
				else
					action = FILE_ACTION_NONE;
			}
			break;
	}

	/* Create a new entry for this file */
	entry = pg_malloc(sizeof(file_entry_t));
	entry->path = pg_strdup(path);
	entry->type = type;
	entry->action = action;
	entry->oldsize = oldsize;
	entry->newsize = newsize;
	entry->link_target = link_target ? pg_strdup(link_target) : NULL;
	entry->next = NULL;
	entry->pagemap.bitmap = NULL;
	entry->pagemap.bitmapsize = 0;

	if (map->last)
	{
		map->last->next = entry;
		map->last = entry;
	}
	else
		map->first = map->last = entry;
	map->nlist++;
}


/*
 * Callback for processing local file list.
 *
 * All remote files must be processed before calling this. This only marks
 * local files that don't exist in the remote system for deletion.
 */
void
process_local_file(const char *path, file_type_t type, size_t oldsize,
				   const char *link_target)
{
	bool		exists;
	char		localpath[MAXPGPATH];
	struct stat statbuf;
	file_entry_t key;
	file_entry_t *key_ptr;
	filemap_t  *map = filemap;
	file_entry_t *entry;

	snprintf(localpath, sizeof(localpath), "%s/%s", datadir_target, path);
	if (lstat(localpath, &statbuf) < 0)
	{
		if (errno == ENOENT)
			exists = false;
		else
		{
			fprintf(stderr, "could not stat file \"%s\": %s",
					localpath, strerror(errno));
			exit(1);
		}
	}

	if (map->array == NULL)
	{
		/* on first call, initialize lookup array */
		if (map->nlist == 0)
		{
			/* should not happen */
			fprintf(stderr, "remote file list is empty\n");
			exit(1);
		}

		filemap_list_to_array();
		qsort(map->array, map->narray, sizeof(file_entry_t *), path_cmp);
	}

	/*
	 * Completely ignore some special files
	 */
	if (strcmp(path, "postmaster.pid") == 0 ||
		strcmp(path, "postmaster.opts") == 0)
		return;

	key.path = (char *) path;
	key_ptr = &key;
	exists = bsearch(&key_ptr, map->array, map->narray, sizeof(file_entry_t *),
					 path_cmp) != NULL;

	/* Remove any file or folder that doesn't exist in the remote system. */
	if (!exists)
	{
		entry = pg_malloc(sizeof(file_entry_t));
		entry->path = pg_strdup(path);
		entry->type = type;
		entry->action = FILE_ACTION_REMOVE;
		entry->oldsize = oldsize;
		entry->newsize = 0;
		entry->link_target = link_target ? pg_strdup(link_target) : NULL;
		entry->next = NULL;
		entry->pagemap.bitmap = NULL;
		entry->pagemap.bitmapsize = 0;

		if (map->last == NULL)
			map->first = entry;
		else
			map->last->next = entry;
		map->last = entry;
		map->nlist++;
	}
	else
	{
		/*
		 * We already handled all files that exist in the remote system
		 * in process_remote_file().
		 */
	}
}

/*
 * This callback gets called while we read the old WAL, for every block that
 * have changed in the local system. It makes note of all the changed blocks
 * in the pagemap of the file.
 */
void
process_block_change(ForkNumber forknum, RelFileNode rnode, BlockNumber blkno)
{
	char	   *path;
	file_entry_t key;
	file_entry_t *key_ptr;
	file_entry_t *entry;
	BlockNumber	blkno_inseg;
	int			segno;
	filemap_t  *map = filemap;

	Assert(filemap->array);

	segno = blkno / RELSEG_SIZE;
	blkno_inseg = blkno % RELSEG_SIZE;

	path = datasegpath(rnode, forknum, segno);

	key.path = (char *) path;
	key_ptr = &key;

	{
		file_entry_t **e = bsearch(&key_ptr, map->array, map->narray,
								   sizeof(file_entry_t *), path_cmp);
		if (e)
			entry = *e;
		else
			entry = NULL;
	}
	free(path);

	if (entry)
	{
		switch (entry->action)
		{
			case FILE_ACTION_NONE:
			case FILE_ACTION_COPY_TAIL:
			case FILE_ACTION_TRUNCATE:
				/* skip if we're truncating away the modified block anyway */
				if ((blkno_inseg + 1) * BLCKSZ <= entry->newsize)
					datapagemap_add(&entry->pagemap, blkno_inseg);
				break;

			case FILE_ACTION_COPY:
			case FILE_ACTION_REMOVE:
				return;

			case FILE_ACTION_CREATE:
				fprintf(stderr, "unexpected page modification for directory or symbolic link \"%s\"", entry->path);
				exit(1);
		}
	}
	else
	{
		/*
		 * If we don't have any record of this file in the file map, it means
		 * that it's a relation that doesn't exist in the remote system, and
		 * it was also subsequently removed in the local system, too. We can
		 * safely ignore it.
		 */
	}
}

/*
 * Convert the linked list of entries in filemap->first/last to the array,
 * filemap->array.
 */
static void
filemap_list_to_array(void)
{
	int			narray;
	file_entry_t *entry,
				*next;

	if (filemap->array == NULL)
		filemap->array = pg_malloc(filemap->nlist * sizeof(file_entry_t));
	else
		filemap->array = pg_realloc(filemap->array,
									(filemap->nlist + filemap->narray) * sizeof(file_entry_t));

	narray = filemap->narray;
	for (entry = filemap->first; entry != NULL; entry = next)
	{
		filemap->array[narray++] = entry;
		next = entry->next;
		entry->next = NULL;
	}
	Assert (narray == filemap->nlist + filemap->narray);
	filemap->narray = narray;
	filemap->nlist = 0;
	filemap->first = filemap->last = NULL;
}

void
filemap_finalize(void)
{
	filemap_list_to_array();
	qsort(filemap->array, filemap->narray, sizeof(file_entry_t *),
		  final_filemap_cmp);
}

static const char *
action_to_str(file_action_t action)
{
	switch (action)
	{
		case FILE_ACTION_NONE:
			return "NONE";
		case FILE_ACTION_COPY:
			return "COPY";
		case FILE_ACTION_TRUNCATE:
			return "TRUNCATE";
		case FILE_ACTION_COPY_TAIL:
			return "COPY_TAIL";
		case FILE_ACTION_CREATE:
			return "CREATE";
		case FILE_ACTION_REMOVE:
			return "REMOVE";

		default:
			return "unknown";
	}
}

void
print_filemap(void)
{
	file_entry_t *entry;
	int			i;

	for (i = 0; i < filemap->narray; i++)
	{
		entry = filemap->array[i];
		if (entry->action != FILE_ACTION_NONE ||
			entry->pagemap.bitmapsize > 0)
		{
			printf("%s (%s)\n", entry->path, action_to_str(entry->action));

			if (entry->pagemap.bitmapsize > 0)
				datapagemap_print(&entry->pagemap);
		}
	}
	fflush(stdout);
}

/*
 * Does it look like a relation data file?
 */
static bool
isRelDataFile(const char *path)
{
	static bool	regexps_compiled = false;
	static regex_t datasegment_regex;
	int			rc;

	/* Compile the regexp if not compiled yet. */
	if (!regexps_compiled)
	{
		/*
		 * Relation data files can be in one of the following directories:
		 *
		 * global/
		 *		shared relations
		 *
		 * base/<db oid>/
		 *		regular relations, default tablespace
		 *
		 * pg_tblspc/<tblspc oid>/PG_9.4_201403261/
		 *		within a non-default tablespace (the name of the directory
		 *		depends on version)
		 *
		 * And the relation data files themselves have a filename like:
		 *
		 * <oid>.<segment number>
		 *
		 * This regular expression tries to capture all of above.
		 */
		const char *datasegment_regex_str =
			"("
			"global"
			"|"
			"base/[0-9]+"
			"|"
			"pg_tblspc/[0-9]+/[PG_0-9.0-9_0-9]+/[0-9]+"
			")/"
			"[0-9]*+$";
		rc = regcomp(&datasegment_regex, datasegment_regex_str, REG_NOSUB | REG_EXTENDED);
		if (rc != 0)
		{
			char errmsg[100];
			regerror(rc, &datasegment_regex, errmsg, sizeof(errmsg));
			fprintf(stderr, "could not compile regular expression: %s\n",
					errmsg);
			exit(1);
		}
	}

	rc = regexec(&datasegment_regex, path, 0, NULL, 0);
	if (rc == 0)
	{
		/* it's a data segment file */
		return true;
	}
	else if (rc != REG_NOMATCH)
	{
		char errmsg[100];
		regerror(rc, &datasegment_regex, errmsg, sizeof(errmsg));
		fprintf(stderr, "could not execute regular expression: %s\n", errmsg);
		exit(1);
	}
	return false;
}

static int
path_cmp(const void *a, const void *b)
{
	file_entry_t *fa = *((file_entry_t **) a);
	file_entry_t *fb = *((file_entry_t **) b);
	return strcmp(fa->path, fb->path);
}

/*
 * In the final stage, the filemap is sorted so that removals come last.
 * From disk space usage point of view, it would be better to do removals
 * first, but for now, safety first. If a whole directory is deleted, all
 * files and subdirectories inside it need to removed first. On creation,
 * parent directory needs to be created before files and directories inside
 * it. To achieve that, the file_action_t enum is ordered so that we can
 * just sort on that first. Furthermore, sort REMOVE entries in reverse
 * path order, so that "foo/bar" subdirectory is removed before "foo".
 */
static int
final_filemap_cmp(const void *a, const void *b)
{
	file_entry_t *fa = *((file_entry_t **) a);
	file_entry_t *fb = *((file_entry_t **) b);

	if (fa->action > fb->action)
		return 1;
	if (fa->action < fb->action)
		return -1;

	if (fa->action == FILE_ACTION_REMOVE)
		return -strcmp(fa->path, fb->path);
	else
		return strcmp(fa->path, fb->path);
}
