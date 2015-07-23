/*-------------------------------------------------------------------------
 *
 * libpq_fetch.c
 *	  Functions for fetching files from a remote server.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2013-2014 VMware, Inc. All Rights Reserved.
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
static void execute_query_or_die(const char *fmt,...)
__attribute__((format(PG_PRINTF_ATTRIBUTE, 1, 2)));

/* variables associated with support bundle */
#define PG_REWIND_SUPPORT_LIB		"$libdir/pg_rewind_support"
#define PG_REWIND_SUPPORT_SCHEMA	"rewind_support"

/*
 * execute_query_or_die
 *
 * Formats a query string from the given arguments and executes the
 * resulting query.  If the query fails, this function logs an error
 * message and calls exit() to kill the program.
 */
static void
execute_query_or_die(const char *fmt,...)
{
	static char command[8192];
	va_list     args;
	PGresult   *result;
	ExecStatusType status;

	Assert(conn != NULL);

	va_start(args, fmt);
	vsnprintf(command, sizeof(command), fmt, args);
	va_end(args);

	result = PQexec(conn, command);
	status = PQresultStatus(result);

	if ((status != PGRES_TUPLES_OK) && (status != PGRES_COMMAND_OK))
	{
		fprintf(stderr, "SQL command failed\n%s\n%s\n", command,
			   PQerrorMessage(conn));
		PQclear(result);
		PQfinish(conn);
		printf("Failure, exiting\n");
		exit(1);
	}

	PQclear(result);
}

void
libpqInitSupport(void)
{
	Assert(conn != NULL);

	/* suppress NOTICE of dropped objects */
	execute_query_or_die("SET client_min_messages = warning;");
	execute_query_or_die("DROP SCHEMA IF EXISTS %s CASCADE;",
						 PG_REWIND_SUPPORT_SCHEMA);
	execute_query_or_die("SET client_min_messages = warning;");
	execute_query_or_die("CREATE SCHEMA %s", PG_REWIND_SUPPORT_SCHEMA);

	/* Create functions needed */
	execute_query_or_die("CREATE OR REPLACE FUNCTION "
						 "%s.rewind_support_ls_dir(text, boolean) "
						 "RETURNS SETOF text "
						 "AS '%s' "
						 "LANGUAGE C STRICT;",
						 PG_REWIND_SUPPORT_SCHEMA,
						 PG_REWIND_SUPPORT_LIB);
	execute_query_or_die("CREATE OR REPLACE FUNCTION "
						 "%s.rewind_support_read_binary_file(text, "
						 "bigint, bigint, boolean) "
						 "RETURNS bytea "
						 "AS '%s' "
						 "LANGUAGE C STRICT;",
						 PG_REWIND_SUPPORT_SCHEMA,
						 PG_REWIND_SUPPORT_LIB);
	execute_query_or_die("CREATE OR REPLACE FUNCTION "
						 "%s.rewind_support_stat_file( "
						 "IN filename text, "
						 "IN missing_ok boolean, "
						 "OUT size bigint, "
						 "OUT access timestamp with time zone, "
						 "OUT modification timestamp with time zone, "
						 "OUT change timestamp with time zone, "
						 "OUT creation timestamp with time zone, "
						 "OUT isdir boolean) "
						 "RETURNS record "
						 "AS '%s' "
						 "LANGUAGE C STRICT;",
						 PG_REWIND_SUPPORT_SCHEMA,
						 PG_REWIND_SUPPORT_LIB);
}

void
libpqFinishSupport(void)
{
	Assert(conn != NULL);
	/* Suppress NOTICE of dropped objects */
	execute_query_or_die("SET client_min_messages = warning;");
	execute_query_or_die("DROP SCHEMA %s CASCADE;", PG_REWIND_SUPPORT_SCHEMA);
	execute_query_or_die("RESET client_min_messages;");
}

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
	char		sql[2048];
	int			i;

	/*
	 * Create a recursive directory listing of the whole data directory.
	 * Using the cte, fetch a listing of the all the files.
	 * For tablespaces, use pg_tablespace_location() function to fetch
	 * the link target (there is no backend function to get a symbolic
	 * link's target in general, so if the admin has put any custom
	 * symbolic links in the data directory, they won't be copied
	 * correctly).
	 */
	snprintf(sql, sizeof(sql),
		"with recursive files (path, filename, size, isdir) as (\n"
		"  select '' as path, filename, size, isdir from\n"
		"  (select %s.rewind_support_ls_dir('.', true) as filename) as fn,\n"
		"        %s.rewind_support_stat_file(fn.filename, true) as this\n"
		"  union all\n"
		"  select parent.path || parent.filename || '/' as path,\n"
		"         fn, this.size, this.isdir\n"
		"  from files as parent,\n"
		"       %s.rewind_support_ls_dir(parent.path || parent.filename, true) as fn,\n"
		"       %s.rewind_support_stat_file(parent.path || parent.filename || '/' || fn, true) as this\n"
		"       where parent.isdir = 't'\n"
		")\n"
		"select path || filename, size, isdir,\n"
		"       pg_tablespace_location(pg_tablespace.oid) as link_target\n"
		"from files\n"
		"left outer join pg_tablespace on files.path = 'pg_tblspc/'\n"
		"                             and oid::text = files.filename\n",
			 PG_REWIND_SUPPORT_SCHEMA,
			 PG_REWIND_SUPPORT_SCHEMA,
			 PG_REWIND_SUPPORT_SCHEMA,
			 PG_REWIND_SUPPORT_SCHEMA);
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{
		fprintf(stderr, "unexpected result while fetching file list: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

	/* sanity check the result set */
	if (!(PQnfields(res) == 4))
	{
		fprintf(stderr, "unexpected result set while fetching file list\n");
		exit(1);
	}

	/* Read result to local variables */
	for (i = 0; i < PQntuples(res); i++)
	{
		char	   *path = PQgetvalue(res, i, 0);
		int			filesize = atoi(PQgetvalue(res, i, 1));
		bool		isdir = (strcmp(PQgetvalue(res, i, 2), "t") == 0);
		char	   *link_target = PQgetvalue(res, i, 3);
		file_type_t type;

		if (PQgetisnull(res, 0, 1))
		{
			/*
			 * The file was removed from the server while the query was
			 * running. Ignore it.
			 */
			continue;
		}

		if (link_target[0])
			type = FILE_TYPE_SYMLINK;
		else if (isdir)
			type = FILE_TYPE_DIRECTORY;
		else
			type = FILE_TYPE_REGULAR;

		process_remote_file(path, type, filesize, link_target);
	}

	PQclear(res);
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

		if (!(!PQgetisnull(res, 0, 0) &&
			  !PQgetisnull(res, 0, 1) &&
			  PQgetlength(res, 0, 1) == sizeof(int32)))
		{
			fprintf(stderr, "unexpected result set while fetching remote files\n");
			exit(1);
		}

		filenamelen = PQgetlength(res, 0, 0);
		filename = pg_malloc(filenamelen + 1);
		memcpy(filename, PQgetvalue(res, 0, 0), filenamelen);
		filename[filenamelen] = '\0';

		/*
		 * It's possible that the file was deleted on remote side after we
		 * created the file map. In this case simply ignore it, as if it was
		 * not there in the first place, and move on.
		 */
		if (PQgetisnull(res, 0, 2))
		{
			fprintf(stderr,
				"received NULL chunk for file \"%s\", file has been deleted\n",
				filename);
			pg_free(filename);
			PQclear(res);
			continue;
		}

		/* Read result set to local variables */
		memcpy(&chunkoff, PQgetvalue(res, 0, 1), sizeof(int32));
		chunkoff = ntohl(chunkoff);
		chunksize = PQgetlength(res, 0, 2);

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
	PQclear(res);
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
	char		sql[1024];
	PGresult   *res;
	int			i;

	/*
	 * First create a temporary table, and load it with the blocks that
	 * we need to fetch.
	 */
	snprintf(sql, sizeof(sql),
		"create temporary table fetchchunks(path text, begin int4, len int4);");
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		fprintf(stderr, "error creating temporary table: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

	snprintf(sql, sizeof(sql),
			 "copy fetchchunks from stdin");
	res = PQexec(conn, sql);

	if (PQresultStatus(res) != PGRES_COPY_IN)
	{
		fprintf(stderr, "unexpected result while sending file list: %s\n",
				PQresultErrorMessage(res));
		exit(1);
	}

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
				/* Truncate the old file out of the way, if any */
				open_target_file(entry->path, true);
				copy_file_range(entry->path, 0, entry->newsize);
				break;

			case FILE_ACTION_TRUNCATE:
				truncate_target_file(entry->path, entry->newsize);
				break;

			case FILE_ACTION_COPY_TAIL:
				copy_file_range(entry->path, entry->oldsize, entry->newsize);
				break;

			case FILE_ACTION_REMOVE:
				remove_target(entry);
				break;

			case FILE_ACTION_CREATE:
				create_target(entry);
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

	PQclear(res);

	/* Ok, we've sent the file list. Now receive the files */
	snprintf(sql, sizeof(sql),
		"-- fetch all the blocks listed in the temp table.\n"
		"select path, begin, \n"
		"%s.rewind_support_read_binary_file(path, begin, len, true) as chunk\n"
		"from fetchchunks\n", PG_REWIND_SUPPORT_SCHEMA);

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
	free(iter);
}
