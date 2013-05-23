/*-------------------------------------------------------------------------
 *
 * libpq_fetch.c
 *	  Functions for fetching files from a remote server.
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 2013 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "catalog/catalog.h"
#include "catalog/pg_type.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>

#include <libpq-fe.h>

#include "pg_rewind.h"
#include "fetch.h"
#include "filemap.h"
#include "datapagemap.h"

static PGconn *conn = NULL;

#define CHUNKSIZE 1000000

static void receiveFileChunks(const char *sql);
static void execute_pagemap(datapagemap_t *pagemap, const char *path);

void
libpqConnect(const char *connstr)
{
	conn = PQconnectdb(connstr);
	if (PQstatus(conn) == CONNECTION_BAD)
	{
		fprintf(stderr, "could not connect to remote server: %s\n",
				PQerrorMessage(conn));
		exit(1);
	}

	if (verbose)
		printf("connected to remote server\n");
}

/*
 * Get a file list.
 */
void
libpqProcessFileList(void)
{
	PGresult   *res;
	const char *sql;
	int			i;

	sql =
		"-- Create a recursive directory listing of the whole data directory\n"
		"with recursive files (path, size, isdir) as (\n"
		"  select filename as path, size, isdir\n"
		"  from (select pg_ls_dir('.') as filename) as filenames,\n"
		"       pg_stat_file(filename) as this\n"
		"  union all\n"
		"  select parent.path || '/' || filename, this.size, this.isdir\n"
		"  from files as parent,\n"
		"       pg_ls_dir(parent.path) as filename,\n"
		"       pg_stat_file(parent.path || '/' || filename) as this\n"
		"       where parent.isdir = 't'\n"
		")\n"
		"-- Using the cte, fetch all files in chunks.\n"
		"select path, size, isdir from files\n";

	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "unexpected result while fetching file list: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

	/* sanity check the result set */
	if (!(PQnfields(res) == 3))
	{
		fprintf(stderr, "unexpected result set while fetching file list\n");
		exit(1);
	}

	/* Read result to local variables */
	for (i = 0; i < PQntuples(res); i++)
	{
		char *path = PQgetvalue(res, i, 0);
		int filesize = atoi(PQgetvalue(res, i, 1));
		bool isdir = (strcmp(PQgetvalue(res, i, 2), "t") == 0);

		process_remote_file(path, filesize, isdir);
	}
}

/*
 * Runs a query, which returns pieces of files from the remote source data
 * directory, and overwrites the corresponding parts of target files with
 * the received parts. The result set is expected to be of format:
 *
 * path		text	-- path in the data directory, e.g "base/1/123"
 * begin	int4	-- offset within the file
 * chunk	bytea	-- file content
 *
 */
static void
receiveFileChunks(const char *sql)
{
	PGresult   *res;

	if (PQsendQueryParams(conn, sql, 0, NULL, NULL, NULL, NULL, 1) != 1)
	{
		fprintf(stderr, "could not send query: %s\n", PQerrorMessage(conn));
		exit(1);
	}

	if (verbose)
		fprintf(stderr, "getting chunks: %s\n", sql);

	if (PQsetSingleRowMode(conn) != 1)
	{
		fprintf(stderr, "could not set libpq connection to single row mode\n");
		exit(1);
	}

	if (verbose)
		fprintf(stderr, "sent query\n");

	while ((res = PQgetResult(conn)) != NULL)
	{
		char   *filename;
		int		filenamelen;
		int		chunkoff;
		int		chunksize;
		char   *chunk;

		switch(PQresultStatus(res))
		{
			case PGRES_SINGLE_TUPLE:
				break;

			case PGRES_TUPLES_OK:
				continue; /* final zero-row result */
			default:
				fprintf(stderr, "unexpected result while fetching remote files: %s\n",
						PQresultErrorMessage(res));
				exit(1);
		}

		/* sanity check the result set */
		if (!(PQnfields(res) == 3 && PQntuples(res) == 1))
		{
			fprintf(stderr, "unexpected result set size while fetching remote files\n");
			exit(1);
		}

		if (!(PQftype(res, 0) == TEXTOID && PQftype(res, 1) == INT4OID && PQftype(res, 2) == BYTEAOID))
		{
			fprintf(stderr, "unexpected data types in result set while fetching remote files: %u %u %u\n", PQftype(res, 0), PQftype(res, 1), PQftype(res, 2));
			exit(1);
		}
		if (!(PQfformat(res, 0) == 1 && PQfformat(res, 1) == 1 && PQfformat(res, 2) == 1))
		{
			fprintf(stderr, "unexpected result format while fetching remote files\n");
			exit(1);
		}

		if (!(!PQgetisnull(res, 0, 0) && !PQgetisnull(res, 0, 1) && !PQgetisnull(res, 0, 2) &&
			  PQgetlength(res, 0, 1) == sizeof(int32)))
		{
			fprintf(stderr, "unexpected result set while fetching remote files\n");
			exit(1);
		}

		/* Read result set to local variables */
		memcpy(&chunkoff, PQgetvalue(res, 0, 1), sizeof(int32));
		chunkoff = ntohl(chunkoff);
		chunksize = PQgetlength(res, 0, 2);

		filenamelen = PQgetlength(res, 0, 0);
		filename = pg_malloc(filenamelen + 1);
		memcpy(filename, PQgetvalue(res, 0, 0), filenamelen);
		filename[filenamelen] = '\0';

		chunk = PQgetvalue(res, 0, 2);

		if (verbose)
			fprintf(stderr, "received chunk for file \"%s\", off %d, len %d\n",
					filename, chunkoff, chunksize);

		open_target_file(filename, false);

		write_file_range(chunk, chunkoff, chunksize);
	}
}

/*
 * Receive a single file.
 */
char *
libpqGetFile(const char *filename, size_t *filesize)
{
	PGresult   *res;
	char	   *result;
	int			len;
	const char *paramValues[1];
	paramValues[0] = filename;

	res = PQexecParams(conn, "select pg_read_binary_file($1)",
					   1, NULL, paramValues, NULL, NULL, 1);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "unexpected result while fetching remote file \"%s\": %s\n",
				filename, PQresultErrorMessage(res));
		exit(1);
	}


	/* sanity check the result set */
	if (!(PQntuples(res) == 1 && !PQgetisnull(res, 0, 0)))
	{
		fprintf(stderr, "unexpected result set while fetching remote file \"%s\"\n",
				filename);
		exit(1);
	}

	/* Read result to local variables */
	len = PQgetlength(res, 0, 0);
	result = pg_malloc(len + 1);
	memcpy(result, PQgetvalue(res, 0, 0), len);
	result[len] = '\0';

	if (verbose)
		printf("fetched file \"%s\", length %d\n", filename, len);

	if (filesize)
		*filesize = len;
	return result;
}

static void
copy_file_range(const char *path, unsigned int begin, unsigned int end)
{
	char linebuf[MAXPGPATH + 23];

	/* Split the range into CHUNKSIZE chunks */
	while (end - begin > 0)
	{
		unsigned int len;

		if (end - begin > CHUNKSIZE)
			len = CHUNKSIZE;
		else
			len = end - begin;

		snprintf(linebuf, sizeof(linebuf), "%s\t%u\t%u\n", path, begin, len);

		if (PQputCopyData(conn, linebuf, strlen(linebuf)) != 1)
		{
			fprintf(stderr, "error sending COPY data: %s\n",
					PQerrorMessage(conn));
			exit(1);
		}
		begin += len;
	}
}

/*
 * Fetch all changed blocks from remote source data directory.
 */
void
libpq_executeFileMap(filemap_t *map)
{
	file_entry_t *entry;
	const char *sql;
	PGresult   *res;

	/*
	 * First create a temporary table, and load it with the blocks that
	 * we need to fetch.
	 */
	sql = "create temporary table fetchchunks(path text, begin int4, len int4);";
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "error creating temporary table: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

	sql = "copy fetchchunks from stdin";
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		fprintf(stderr, "unexpected result while sending file list: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

	for (entry = map->first; entry != NULL; entry = entry->next)
	{
		execute_pagemap(&entry->pagemap, entry->path);

		switch (entry->action)
		{
			case FILE_ACTION_NONE:
				/* ok, do nothing.. */
				break;

			case FILE_ACTION_COPY:
				/* Truncate the old file out of the way, if any */
				open_target_file(entry->path, true);
				copy_file_range(entry->path, 0, entry->newsize);
				break;

			case FILE_ACTION_REMOVE:
				remove_target_file(entry->path);
				break;

			case FILE_ACTION_TRUNCATE:
				truncate_target_file(entry->path, entry->newsize);
				break;

			case FILE_ACTION_COPY_TAIL:
				copy_file_range(entry->path, entry->oldsize, entry->newsize);
				break;

			case FILE_ACTION_CREATEDIR:
				create_target_dir(entry->path);
				break;
		}
	}

	if (PQputCopyEnd(conn, NULL) != 1)
	{
		fprintf(stderr, "error sending end-of-COPY: %s\n",
				PQerrorMessage(conn));
		exit(1);
	}

	while ((res = PQgetResult(conn)) != NULL)
	{
		if (PQresultStatus(res) != PGRES_COMMAND_OK)
		{
			fprintf(stderr, "unexpected result while sending file list: %s\n",
					PQresultErrorMessage(res));
			exit(1);
		}
	}

	/* Ok, we've sent the file list. Now receive the files */
	sql =
		"-- fetch all the blocks listed in the temp table.\n"
		"select path, begin, \n"
		"  pg_read_binary_file(path, begin, len) as chunk\n"
		"from fetchchunks\n";

	receiveFileChunks(sql);
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

		copy_file_range(path, offset, offset + BLCKSZ);
	}
}
