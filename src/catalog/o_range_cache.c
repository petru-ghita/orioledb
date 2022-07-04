/*-------------------------------------------------------------------------
 *
 * o_range_cache.c
 *		Routines for orioledb range sys cache.
 *
 * range_cache is tree that contains cached range metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_range_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"

#include "access/htup_details.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "catalog/pg_range.h"
#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/fmgrtab.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

static OSysCache *range_cache = NULL;

static void o_range_cache_free_entry(Pointer entry);
static void o_range_cache_fill_entry(Pointer *entry_ptr, Oid datoid,
									 Oid typoid, XLogRecPtr insert_lsn,
									 Pointer arg);

O_SYS_CACHE_FUNCS(range_cache, ORange);

static OSysCacheFuncs range_cache_funcs =
{
	.free_entry = o_range_cache_free_entry,
	.fill_entry = o_range_cache_fill_entry
};

/*
 * Initializes the range sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(range_cache)
{
	range_cache = o_create_sys_cache(SYS_TREES_RANGE_CACHE,
									 false, true,
									 TypeRelationId,
									 fastcache,
									 mcxt,
									 &range_cache_funcs);
}

void
o_range_cache_fill_entry(Pointer *entry_ptr, Oid datoid, Oid typoid,
						 XLogRecPtr insert_lsn, Pointer arg)
{
	TypeCacheEntry *typcache;
	ORange *o_range = (ORange *) *entry_ptr;

	/*
	 * find typecache entry
	 */
	typcache = lookup_type_cache(typoid, TYPECACHE_RANGE_INFO);
	if (typcache->rngelemtype == NULL)
		elog(ERROR, "type %u is not a range type", typoid);

	if (o_range == NULL)
	{
		o_range = palloc0(sizeof(ORange));
		*entry_ptr = (Pointer) o_range;
	}

	custom_type_add_if_needed(datoid,
							  typcache->rngelemtype->type_id,
							  insert_lsn);

	o_type_cache_add_if_needed(datoid, typcache->rngelemtype->type_id,
							   insert_lsn, NULL);
	o_range->elem_type = typcache->rngelemtype->type_id;
	o_range->rng_collation = typcache->rng_collation;
	o_proc_cache_validate_add(datoid, typcache->rng_cmp_proc_finfo.fn_oid,
							  typcache->rng_collation, "comparison",
							  "range field");
	o_range->rng_cmp_oid = typcache->rng_cmp_proc_finfo.fn_oid;
}

void
o_range_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

TypeCacheEntry *
o_range_cmp_hook(FunctionCallInfo fcinfo, Oid rngtypid,
				 MemoryContext mcxt)
{
	TypeCacheEntry *typcache = (TypeCacheEntry *) fcinfo->flinfo->fn_extra;

	if (typcache == NULL ||
		typcache->type_id != rngtypid)
	{
		XLogRecPtr		cur_lsn;
		Oid				datoid;
		ORange		   *o_range;
		MemoryContext	prev_context = MemoryContextSwitchTo(mcxt);

		o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
		o_range = o_range_cache_search(datoid, rngtypid, cur_lsn);
		if (o_range)
		{
			typcache = palloc0(sizeof(TypeCacheEntry));
			typcache->type_id = rngtypid;
			typcache->rngelemtype = palloc0(sizeof(TypeCacheEntry));
			o_type_cache_fill_info(o_range->elem_type,
								   &typcache->rngelemtype->typlen,
								   &typcache->rngelemtype->typbyval,
								   &typcache->rngelemtype->typalign);
			typcache->rng_collation = o_range->rng_collation;

			o_proc_cache_fill_finfo(&typcache->rng_cmp_proc_finfo,
									o_range->rng_cmp_oid);

			fcinfo->flinfo->fn_extra = (void *) typcache;
			MemoryContextSwitchTo(prev_context);
		}
		else
			typcache = NULL;
	}

	return typcache;
}
