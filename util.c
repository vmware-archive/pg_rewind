/*-------------------------------------------------------------------------
 *
 * util.c
 *		Misc utility functions
 *
 * Portions Copyright (c) 2013-2015 VMware, Inc. All Rights Reserved.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "common/relpath.h"
#include "catalog/catalog.h"
#include "catalog/pg_tablespace.h"

#include "pg_rewind.h"

char *
datasegpath(RelFileNode rnode, ForkNumber forknum, BlockNumber segno)
{
	char *path;
	char *segpath;

	path = relpathperm(rnode, forknum);
	if (segno > 0)
	{
		segpath = pg_malloc(strlen(path) + 13);
		sprintf(segpath, "%s.%u", path, segno);
		pg_free(path);
		return segpath;
	}
	else
		return path;
}
