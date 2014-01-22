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

filemap_t *filemap = NULL;

static bool isRelDataFile(const char *path);
static int path_cmp(const void *a, const void *b);


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
	map->array = NULL;
	map->nfiles = 0;

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
process_remote_file(const char *path, size_t newsize, bool isdir)
{
	bool		exists;
	char		localpath[MAXPGPATH];
	struct stat statbuf;
	filemap_t  *map = filemap;
	file_action_t action;
	size_t		oldsize;
	file_entry_t *entry;

	Assert(map->array == NULL);

	/*
	 * Ignore some special files
	 */
	if (strcmp(path, "postmaster.pid") == 0)
		return;
	/* PG_VERSIONs should be identical, but avoid overwriting it for paranoia */
	if (endswith(path, "PG_VERSION"))
		return;

	/* Does the corresponding local file exist? */
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
	else if (isdir && !S_ISDIR(statbuf.st_mode))
	{
		/* it's a directory in target, but not in source. Strange.. */
		fprintf(stderr, "\"%s\" is not a directory.\n", localpath);
		exit(1);
	}
	else if (!isdir && S_ISDIR(statbuf.st_mode))
	{
		/* it's a directory in source, but not in target. Strange.. */
		fprintf(stderr, "\"%s\" is not a regular file.\n", localpath);
		exit(1);
	}
	else if (!S_ISREG(statbuf.st_mode) && !S_ISDIR(statbuf.st_mode))
	{
		/* not a file, and not a directory. */
		/* TODO I think we need to handle symbolic links here */
		fprintf(stderr, "\"%s\" is not a regular file.\n", localpath);
		exit(1);
	}
	else
		exists = true;

	if (isdir)
	{
		/* sanity check */
		if (isRelDataFile(path))
		{
			fprintf(stderr, "data file in source \"%s\" is a directory.\n", path);
			exit(1);
		}

		if (!exists)
			action = FILE_ACTION_CREATEDIR;
		else
			action = FILE_ACTION_NONE;
		oldsize = 0;
	}
	else if (!exists || !isRelDataFile(path))
	{
		/*
		 * File exists in source, but not in target. Or it's a non-data file
		 * that we have no special processing for. Copy it in toto.
		 */
		action = FILE_ACTION_COPY;
		oldsize = 0;
	}
	else
	{
		/*
		 * It's a data file that exists in both.
		 *
		 * If it's smaller in target, we can truncate it. There will also be
		 * a WAL record of the truncation in the source system, so WAL replay
		 * would eventually truncate the target too, but we might as well do
		 * it now.
		 *
		 * If it's larger in the target, it means that it has been truncated
		 * in the target, or enlarged in the source, or both. If it was
		 * truncated locally, we need to copy the missing tail from the remote
		 * system. If it was enlarged in the remote system, there will be WAL
		 * records in the remote system for the new blocks, so we wouldn't
		 * need to copy them here. But we don't know which scenario we're
		 * dealing with, and there's no harm in copying the missing blocks
		 * now, so do it now.
		 *
		 * If it's the same size, do nothing here. Any locally modified blocks
		 * will be copied based on parsing the local WAL, and any remotely
		 * modified blocks will be updated after rewinding, when the remote
		 * WAL is replayed.
		 */
		oldsize = statbuf.st_size;
		if (oldsize < newsize)
			action = FILE_ACTION_COPY_TAIL;
		else if (oldsize > newsize)
			action = FILE_ACTION_TRUNCATE;
		else
			action = FILE_ACTION_NONE;
	}

	/* Create a new entry for this file */
	entry = pg_malloc(sizeof(file_entry_t));
	entry->path = pg_strdup(path);
	entry->isdir = isdir;
	entry->action = action;
	entry->oldsize = oldsize;
	entry->newsize = newsize;
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
	map->nfiles++;
}


/*
 * Callback for processing local file list.
 *
 * All remote files must be processed before calling this. This only marks
 * local files that don't exist in the remote system for deletion.
 */
void
process_local_file(const char *path, size_t oldsize, bool isdir)
{
	bool		exists;
	file_entry_t key;
	file_entry_t *key_ptr;
	filemap_t  *map = filemap;
	file_action_t action;
	file_entry_t *entry;

	if (map->nfiles == 0)
	{
		/* should not happen */
		fprintf(stderr, "remote file list is empty\n");
		exit(1);
	}
	if (map->array == NULL)
	{
		/* on first call, initialize lookup array */
		int i;
		map->array = pg_malloc(map->nfiles * sizeof(file_entry_t));

		i = 0;
		for (entry = map->first; entry != NULL; entry = entry->next)
			map->array[i++] = entry;
		Assert (i == map->nfiles);

		qsort(map->array, map->nfiles, sizeof(file_entry_t *), path_cmp);
	}

	/*
	 * Ignore some special files
	 */
	if (strcmp(path, "postmaster.pid") == 0)
		return;
	/* PG_VERSIONs should be identical, but avoid overwriting it for paranoia */
	if (endswith(path, "PG_VERSION"))
		return;

	key.path = (char *) path;
	key_ptr = &key;
	exists = bsearch(&key_ptr, map->array, map->nfiles, sizeof(file_entry_t *),
					 path_cmp) != NULL;

	/* Remove any file that doesn't exist in the remote system. */
	if (!exists)
	{
		action = FILE_ACTION_REMOVE;

		/* Create a new entry for this file */
		entry = pg_malloc(sizeof(file_entry_t));
		entry->path = pg_strdup(path);
		entry->isdir = isdir;
		entry->action = action;
		entry->oldsize = oldsize;
		entry->newsize = 0;
		entry->next = NULL;
		entry->pagemap.bitmap = NULL;
		entry->pagemap.bitmapsize = 0;

		map->last->next = entry;
		map->last = entry;
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
 * This callback  gets called while we read the old WAL, for every block that
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
		file_entry_t **e = bsearch(&key_ptr, map->array, map->nfiles,
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

			case FILE_ACTION_CREATEDIR:
				fprintf(stderr, "unexpected page modification for directory \"%s\"", entry->path);
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


static const char *
action_to_str(file_action_t action)
{
	switch (action)
	{
		case FILE_ACTION_NONE:
			return "NONE";
		case FILE_ACTION_COPY:
			return "COPY";
		case FILE_ACTION_REMOVE:
			return "REMOVE";
		case FILE_ACTION_TRUNCATE:
			return "TRUNCATE";
		case FILE_ACTION_COPY_TAIL:
			return "COPY_TAIL";
		case FILE_ACTION_CREATEDIR:
			return "CREATEDIR";

		default:
			return "unknown";
	}
}

void
print_filemap(void)
{
	file_entry_t *entry;

	for (entry = filemap->first; entry != NULL; entry = entry->next)
	{
		if (entry->action != FILE_ACTION_NONE ||
			entry->pagemap.bitmapsize > 0)
		{
			printf("%s (%s)\n", entry->path, action_to_str(entry->action));

			if (entry->pagemap.bitmapsize > 0)
				datapagemap_print(&entry->pagemap);
		}
	}
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
		/* If you change this, also update the regexp in libpq_fetch.c */
		const char *datasegment_regex_str = "(global|base/[0-9]+)/[0-9]+$";
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
