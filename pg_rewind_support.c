/*-------------------------------------------------------------------------
 * pg_rewind_support.c
 *	  Set of server-side functions for support of pg_rewind. This set of
 *    functions is more widely available in PostgreSQL 9.5 and onwards
 *    to be able to manage correctly temporary files generated on a source
 *    server when rewinding using a stream connection. A minimum set of
 *    option coverage is used on purpose to minimize the amount of objects
 *    created when running pg_rewing in stream mode.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 2013-2014 VMware, Inc. All Rights Reserved.
 *-------------------------------------------------------------------------
 */

#ifdef FRONTEND
#undef FRONTEND
#endif

#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>

#include "access/htup_details.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "catalog/pg_type.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

typedef struct
{
	char	   *location;
	DIR		   *dirdesc;
} directory_fctx;

PG_FUNCTION_INFO_V1(rewind_support_ls_dir);
PG_FUNCTION_INFO_V1(rewind_support_stat_file);
PG_FUNCTION_INFO_V1(rewind_support_read_binary_file);

/*
 * Convert a "text" filename argument to C string, and check it's allowable.
 *
 * Filename may be absolute or relative to the DataDir, but we only allow
 * absolute paths that match DataDir or Log_directory.
 */
static char *
convert_and_check_filename(text *arg)
{
	char	   *filename;

	filename = text_to_cstring(arg);
	canonicalize_path(filename);	/* filename can change length here */

	if (is_absolute_path(filename))
	{
		/* Disallow '/a/b/data/..' */
		if (path_contains_parent_reference(filename))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
			(errmsg("reference to parent directory (\"..\") not allowed"))));

		/*
		 * Allow absolute paths if within DataDir or Log_directory, even
		 * though Log_directory might be outside DataDir.
		 */
		if (!path_is_prefix_of_path(DataDir, filename) &&
			(!is_absolute_path(Log_directory) ||
			 !path_is_prefix_of_path(Log_directory, filename)))
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 (errmsg("absolute path not allowed"))));
	}
	else if (!path_is_relative_and_below_cwd(filename))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("path must be in or below the current directory"))));

	return filename;
}

/*
 * Read a section of a file, returning it as bytea
 *
 * Caller is responsible for all permissions checking.
 *
 * We read the whole of the file when bytes_to_read is negative.
 */
static bytea *
local_read_binary_file(const char *filename,
					   int64 seek_offset,
					   int64 bytes_to_read,
					   bool missing_ok)
{
	bytea	   *buf;
	size_t		nbytes;
	FILE	   *file;

	if (bytes_to_read < 0)
	{
		if (seek_offset < 0)
			bytes_to_read = -seek_offset;
		else
		{
			struct stat fst;

			if (stat(filename, &fst) < 0)
			{
				if (missing_ok && errno == ENOENT)
					return NULL;
				else
					ereport(ERROR,
							(errcode_for_file_access(),
						errmsg("could not stat file \"%s\": %m", filename)));
			}

			bytes_to_read = fst.st_size - seek_offset;
		}
	}

	/* not sure why anyone thought that int64 length was a good idea */
	if (bytes_to_read > (MaxAllocSize - VARHDRSZ))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("requested length too large")));

	if ((file = AllocateFile(filename, PG_BINARY_R)) == NULL)
	{
		if (missing_ok && errno == ENOENT)
			return NULL;
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\" for reading: %m",
							filename)));
	}

	if (fseeko(file, (off_t) seek_offset,
			   (seek_offset >= 0) ? SEEK_SET : SEEK_END) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not seek in file \"%s\": %m", filename)));

	buf = (bytea *) palloc((Size) bytes_to_read + VARHDRSZ);

	nbytes = fread(VARDATA(buf), 1, (size_t) bytes_to_read, file);

	if (ferror(file))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));

	SET_VARSIZE(buf, nbytes + VARHDRSZ);

	FreeFile(file);

	return buf;
}

/*
 * rewind_support_read_binary_file
 *
 * Read a section of a file, returning it as bytea. This function supports
 * a unique grammar:
 * read_binary_file(file, offset, len, missing_ok)
 * Be careful that bytes_to_read = -1 means that the whole file is read
 * back.
 */
Datum
rewind_support_read_binary_file(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_P(0);
	int64		seek_offset = 0;
	int64		bytes_to_read = -1;
	bool		missing_ok = false;
	char	   *filename;
	bytea	   *result;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to read files"))));

	seek_offset = PG_GETARG_INT64(1);
	bytes_to_read = PG_GETARG_INT64(2);
	missing_ok = PG_GETARG_BOOL(3);

	filename = convert_and_check_filename(filename_t);

	result = local_read_binary_file(filename, seek_offset,
									bytes_to_read, missing_ok);
	if (result)
		PG_RETURN_BYTEA_P(result);
	else
		PG_RETURN_NULL();
}

/*
 * rewind_support_ls_dir
 *
 * List a directory (returns the filenames only). This function
 * supports a unique grammar:
 * rewind_support_ls_dir(dirname, missing_ok)
 */
Datum
rewind_support_ls_dir(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	struct dirent *de;
	directory_fctx *fctx;
	MemoryContext oldcontext;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to get directory listings"))));

	if (SRF_IS_FIRSTCALL())
	{
		bool		missing_ok = false;

		missing_ok = PG_GETARG_BOOL(1);

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		fctx = palloc(sizeof(directory_fctx));
		fctx->location = convert_and_check_filename(PG_GETARG_TEXT_P(0));

		fctx->dirdesc = AllocateDir(fctx->location);

		if (!fctx->dirdesc)
		{
			if (missing_ok && errno == ENOENT)
			{
				MemoryContextSwitchTo(oldcontext);
				SRF_RETURN_DONE(funcctx);
			}
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not open directory \"%s\": %m",
								fctx->location)));
		}
		funcctx->user_fctx = fctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	fctx = (directory_fctx *) funcctx->user_fctx;

	while ((de = ReadDir(fctx->dirdesc, fctx->location)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		SRF_RETURN_NEXT(funcctx, CStringGetTextDatum(de->d_name));
	}

	FreeDir(fctx->dirdesc);

	SRF_RETURN_DONE(funcctx);
}

/*
 * rewind_support_stat_file
 *
 * Stat a file. This function supports only one grammar:
 * rewind_support_stat_file(filename, missing_ok)
 */
Datum
rewind_support_stat_file(PG_FUNCTION_ARGS)
{
	text	   *filename_t = PG_GETARG_TEXT_P(0);
	char	   *filename;
	struct stat fst;
	Datum		values[6];
	bool		isnull[6];
	HeapTuple	tuple;
	TupleDesc	tupdesc;
	bool		missing_ok = false;

	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 (errmsg("must be superuser to get file information"))));

	/* check the optional argument */
	missing_ok = PG_GETARG_BOOL(1);

	filename = convert_and_check_filename(filename_t);

	if (stat(filename, &fst) < 0)
	{
		if (missing_ok && errno == ENOENT)
			PG_RETURN_NULL();
		else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not stat file \"%s\": %m", filename)));
	}

	/*
	 * This record type had better match the output parameters declared for me
	 * in pg_proc.h.
	 */
	tupdesc = CreateTemplateTupleDesc(6, false);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1,
					   "size", INT8OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2,
					   "access", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3,
					   "modification", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4,
					   "change", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5,
					   "creation", TIMESTAMPTZOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 6,
					   "isdir", BOOLOID, -1, 0);
	BlessTupleDesc(tupdesc);

	memset(isnull, false, sizeof(isnull));

	values[0] = Int64GetDatum((int64) fst.st_size);
	values[1] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_atime));
	values[2] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_mtime));
	/* Unix has file status change time, while Win32 has creation time */
#if !defined(WIN32) && !defined(__CYGWIN__)
	values[3] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
	isnull[4] = true;
#else
	isnull[3] = true;
	values[4] = TimestampTzGetDatum(time_t_to_timestamptz(fst.st_ctime));
#endif
	values[5] = BoolGetDatum(S_ISDIR(fst.st_mode));

	tuple = heap_form_tuple(tupdesc, values, isnull);

	pfree(filename);

	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
