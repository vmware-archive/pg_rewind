/*-------------------------------------------------------------------------
 *
 * copy_fetch.c
 *	  Functions for copying a PostgreSQL data directory
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/catalog.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#include "pg_rewind.h"
#include "fetch.h"
#include "filemap.h"
#include "datapagemap.h"
#include "util.h"

static void recurse_dir(const char *datadir, const char *path,
			process_file_callback_t callback);

static void execute_pagemap(datapagemap_t *pagemap, const char *path);

static void create_target_dir(const char *path);
static void remove_target_dir(const char *path);
static void create_target_symlink(const char *path, const char *link);
static void remove_target_symlink(const char *path);

/*
 * Traverse through all files in a data directory, calling 'callback'
 * for each file.
 */
void
traverse_datadir(const char *datadir, process_file_callback_t callback)
{
	/* should this copy config files or not? */
	recurse_dir(datadir, NULL, callback);
}

/*
 * recursive part of traverse_datadir
 *
 * parent_path is the current subdirectory's path relative to datadir,
 * or NULL at the top level.
 */
static void
recurse_dir(const char *datadir, const char *parentpath,
			process_file_callback_t callback)
{
	DIR		   *xldir;
	struct dirent *xlde;
	char		fullparentpath[MAXPGPATH];

	if (parentpath)
		snprintf(fullparentpath, MAXPGPATH, "%s/%s", datadir, parentpath);
	else
		snprintf(fullparentpath, MAXPGPATH, "%s", datadir);

	xldir = opendir(fullparentpath);
	if (xldir == NULL)
	{
		fprintf(stderr, "could not open directory \"%s\": %s\n",
				fullparentpath, strerror(errno));
		exit(1);
	}

	while ((xlde = readdir(xldir)) != NULL)
	{
		struct stat fst;
		char		fullpath[MAXPGPATH];
		char		path[MAXPGPATH];

		if (strcmp(xlde->d_name, ".") == 0 ||
			strcmp(xlde->d_name, "..") == 0)
			continue;

		snprintf(fullpath, MAXPGPATH, "%s/%s", fullparentpath, xlde->d_name);

		if (lstat(fullpath, &fst) < 0)
		{
			fprintf(stderr, "warning: could not stat file \"%s\": %s",
					fullpath, strerror(errno));
			/*
			 * This is ok, if the new master is running and the file was
			 * just removed. If it was a data file, there should be a WAL
			 * record of the removal. If it was something else, it couldn't
			 * have been critical anyway.
			 *
			 * TODO: But complain if we're processing the target dir!
			 */
		}

		if (parentpath)
			snprintf(path, MAXPGPATH, "%s/%s", parentpath, xlde->d_name);
		else
			snprintf(path, MAXPGPATH, "%s", xlde->d_name);

		if (S_ISREG(fst.st_mode))
			callback(path, FILE_TYPE_REGULAR, fst.st_size, NULL);
		else if (S_ISDIR(fst.st_mode))
		{
			callback(path, FILE_TYPE_DIRECTORY, 0, NULL);
			/* recurse to handle subdirectories */
			recurse_dir(datadir, path, callback);
		}
		else if (S_ISLNK(fst.st_mode))
		{
			char		link_target[MAXPGPATH];
			int			len;

			len = readlink(fullpath, link_target, sizeof(link_target));
			if (len < 0)
			{
				fprintf(stderr, "readlink() failed on \"%s\": %s\n",
						fullpath, strerror(errno));
				exit(1);
			}
			if (len >= sizeof(link_target))
			{
				fprintf(stderr, "symbolic link \"%s\" target path too long\n",
						fullpath);
				exit(1);
			}
			link_target[len] = '\0';

			callback(path, FILE_TYPE_SYMLINK, 0, link_target);

			/*
			 * If it's a symlink within pg_tblspc, we need to recurse into it,
			 * to process all the tablespaces.  We also follow a symlink if
			 * it's for pg_xlog.  Symlinks elsewhere are ignored.
			 */
			if ((parentpath && strcmp(parentpath, "pg_tblspc") == 0) ||
				strcmp(path, "pg_xlog") == 0)
				recurse_dir(datadir, path, callback);
		}
	}
	closedir(xldir);
}

static int dstfd = -1;
static char dstpath[MAXPGPATH] = "";

void
open_target_file(const char *path, bool trunc)
{
	int			mode;

	if (dry_run)
		return;

	if (dstfd != -1 && !trunc &&
		strcmp(path, &dstpath[strlen(datadir_target) + 1]) == 0)
		return; /* already open */

	if (dstfd != -1)
		close_target_file();

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);

	mode = O_WRONLY | O_CREAT | PG_BINARY;
	if (trunc)
		mode |= O_TRUNC;
	dstfd = open(dstpath, mode, 0600);
	if (dstfd < 0)
	{
		fprintf(stderr, "could not open destination file \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}

void
close_target_file(void)
{
	if (close(dstfd) != 0)
	{
		fprintf(stderr, "error closing destination file \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}

	dstfd = -1;

	/* fsync is done globally at the end of processing */
}

void
write_file_range(char *buf, off_t begin, size_t size)
{
	int			writeleft;
	char	   *p;

	if (dry_run)
		return;

	if (lseek(dstfd, begin, SEEK_SET) == -1)
	{
		fprintf(stderr, "could not seek in destination file \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}

	writeleft = size;
	p = buf;
	while (writeleft > 0)
	{
		int		writelen;

		writelen = write(dstfd, p, writeleft);
		if (writelen < 0)
		{
			fprintf(stderr, "could not write file \"%s\": %s\n",
					dstpath, strerror(errno));
			exit(1);
		}

		p += writelen;
		writeleft -= writelen;
	}

	/* keep the file open, in case we need to copy more blocks in it */
}


/*
 * Copy a file from source to target, between 'begin' and 'end' offsets.
 */
static void
rewind_copy_file_range(const char *path, off_t begin, off_t end, bool trunc)
{
	char		buf[BLCKSZ];
	char		srcpath[MAXPGPATH];
	int			srcfd;

	snprintf(srcpath, sizeof(srcpath), "%s/%s", datadir_source, path);

	srcfd = open(srcpath, O_RDONLY | PG_BINARY, 0);
	if (srcfd < 0)
	{
		fprintf(stderr, "could not open source file \"%s\": %s\n", srcpath, strerror(errno));
		exit(1);
	}

	if (lseek(srcfd, begin, SEEK_SET) == -1)
	{
		fprintf(stderr, "could not seek in source file: %s\n", strerror(errno));
		exit(1);
	}

	open_target_file(path, trunc);

	while (end - begin > 0)
	{
		int		readlen;
		int		len;

		if (end - begin > sizeof(buf))
			len = sizeof(buf);
		else
			len = end - begin;

		readlen = read(srcfd, buf, len);

		if (readlen < 0)
		{
			fprintf(stderr, "could not read file \"%s\": %s\n", srcpath, strerror(errno));
			exit(1);
		}
		else if (readlen == 0)
		{
			fprintf(stderr, "unexpected EOF while reading file \"%s\"\n", srcpath);
			exit(1);
		}

		write_file_range(buf, begin, readlen);
		begin += readlen;
	}

	if (close(srcfd) != 0)
		fprintf(stderr, "error closing file \"%s\": %s\n", srcpath, strerror(errno));
}

/*
 * Checks if two file descriptors point to the same file. This is used as
 * a sanity check, to make sure the user doesn't try to copy a data directory
 * over itself.
 */
void
check_samefile(int fd1, int fd2)
{
	struct stat statbuf1,
				statbuf2;

	if (fstat(fd1, &statbuf1) < 0)
	{
		fprintf(stderr, "fstat failed: %s\n", strerror(errno));
		exit(1);
	}

	if (fstat(fd2, &statbuf2) < 0)
	{
		fprintf(stderr, "fstat failed: %s\n", strerror(errno));
		exit(1);
	}

	if (statbuf1.st_dev == statbuf2.st_dev &&
		statbuf1.st_ino == statbuf2.st_ino)
	{
		fprintf(stderr, "old and new data directory are the same\n");
		exit(1);
	}
}

/*
 * Copy all relation data files from datadir_source to datadir_target, which
 * are marked in the given data page map.
 */
void
copy_executeFileMap(filemap_t *map)
{
	file_entry_t *entry;
	int			i;

	for (i = 0; i < map->narray; i++)
	{
		entry = map->array[i];
		execute_pagemap(&entry->pagemap, entry->path);

		switch (entry->action)
		{
			case FILE_ACTION_NONE:
				/* ok, do nothing.. */
				break;

			case FILE_ACTION_COPY:
				rewind_copy_file_range(entry->path, 0, entry->newsize, true);
				break;

			case FILE_ACTION_TRUNCATE:
				truncate_target_file(entry->path, entry->newsize);
				break;

			case FILE_ACTION_COPY_TAIL:
				rewind_copy_file_range(entry->path, entry->oldsize, entry->newsize, false);
				break;

			case FILE_ACTION_CREATE:
				create_target(entry);
				break;

			case FILE_ACTION_REMOVE:
				remove_target(entry);
				break;
		}
	}

	if (dstfd != -1)
		close_target_file();
}


void
remove_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_REMOVE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			remove_target_dir(entry->path);
			break;

		case FILE_TYPE_REGULAR:
			remove_target_file(entry->path, false);
			break;

		case FILE_TYPE_SYMLINK:
			remove_target_symlink(entry->path);
			break;
	}
}

void
create_target(file_entry_t *entry)
{
	Assert(entry->action == FILE_ACTION_CREATE);

	switch (entry->type)
	{
		case FILE_TYPE_DIRECTORY:
			create_target_dir(entry->path);
			break;

		case FILE_TYPE_SYMLINK:
			create_target_symlink(entry->path, entry->link_target);
			break;

		case FILE_TYPE_REGULAR:
			/* can't happen */
			fprintf (stderr, "invalid action (CREATE) for regular file\n");
			exit(1);
			break;
	}
}

/*
 * Remove a file from target data directory.  If missing_ok is true, it
 * is fine for the target file to not exist.
 */
void
remove_target_file(const char *path, bool missing_ok)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
	{
		if (errno == ENOENT && missing_ok)
			return;

		fprintf(stderr, "could not remove file \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}

void
truncate_target_file(const char *path, off_t newsize)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (truncate(dstpath, newsize) != 0)
	{
		fprintf(stderr, "could not truncate file \"%s\" to %u bytes: %s\n",
				dstpath, (unsigned int) newsize, strerror(errno));
		exit(1);
	}
}

static void
create_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (mkdir(dstpath, S_IRWXU) != 0)
	{
		fprintf(stderr, "could not create directory \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}

static void
remove_target_dir(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (rmdir(dstpath) != 0)
	{
		fprintf(stderr, "could not remove directory \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}

static void
create_target_symlink(const char *path, const char *link)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (symlink(link, dstpath) != 0)
	{
		fprintf(stderr, "could not create symbolic link at \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}

static void
remove_target_symlink(const char *path)
{
	char		dstpath[MAXPGPATH];

	if (dry_run)
		return;

	snprintf(dstpath, sizeof(dstpath), "%s/%s", datadir_target, path);
	if (unlink(dstpath) != 0)
	{
		fprintf(stderr, "could not remove symbolic link \"%s\": %s\n",
				dstpath, strerror(errno));
		exit(1);
	}
}


static void
execute_pagemap(datapagemap_t *pagemap, const char *path)
{
	datapagemap_iterator_t *iter;
	BlockNumber blkno;

	iter = datapagemap_iterate(pagemap);
	while (datapagemap_next(iter, &blkno))
	{
		off_t offset = blkno * BLCKSZ;

		rewind_copy_file_range(path, offset, offset + BLCKSZ, false);
		/* Ok, this block has now been copied from new data dir to old */
	}
	free(iter);
}

/*
 * Read a file into memory. The file to be read is <datadir>/<path>.
 * The file contents are returned in a malloc'd buffer, and *filesize
 * is set to the length of the file.
 *
 * The returned buffer is always zero-terminated; the size of the returned
 * buffer is actually *filesize + 1. That's handy when reading a text file.
 * This function can be used to read binary files as well, you can just
 * ignore the zero-terminator in that case.
 *
 * This function is used to implement the fetchFile function in the "fetch"
 * interface (see fetch.c), but is also called directly.
 */
char *
slurpFile(const char *datadir, const char *path, size_t *filesize)
{
	int			fd;
	char	   *buffer;
	struct stat statbuf;
	char		fullpath[MAXPGPATH];
	int			len;

	snprintf(fullpath, sizeof(fullpath), "%s/%s", datadir, path);

	if ((fd = open(fullpath, O_RDONLY | PG_BINARY, 0)) == -1)
	{
		fprintf(stderr, _("could not open file \"%s\" for reading: %s\n"),
				fullpath, strerror(errno));
		exit(2);
	}

	if (fstat(fd, &statbuf) < 0)
	{
		fprintf(stderr, _("could not open file \"%s\" for reading: %s\n"),
				fullpath, strerror(errno));
		exit(2);
	}

	len = statbuf.st_size;

	buffer = pg_malloc(len + 1);

	if (read(fd, buffer, len) != len)
	{
		fprintf(stderr, _("could not read file \"%s\": %s\n"),
				fullpath, strerror(errno));
		exit(2);
	}
	close(fd);

	/* Zero-terminate the buffer. */
	buffer[len] = '\0';

	if (filesize)
		*filesize = len;
	return buffer;
}
