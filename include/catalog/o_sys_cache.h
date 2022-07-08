/*-------------------------------------------------------------------------
 *
 * o_sys_cache.h
 *		Generic interface for system catalog duplicate trees.
 *
 * Generic system catalog tree interface that used to prevent syscache
 * usage during recovery. System catalog cache trees shoud use o_sys_cache_*
 * functions in sysTreesMeta (sys_trees.c), but if sys cache is
 * not TOAST tup_print function should be also provided.
 * Sys cache lookups are also cached in local backend mamory.
 * Cache entry invalidation is performed by syscache hook.
 * Instead of physical deletion of sys cache entry we mark it as deleted.
 * Normally only not deleted entries used. During recovery we use
 * sys cache entries accroding to current WAL position.
 * Physical deletion of deleted values is performed during checkpoint,
 * which is also called after successed recovery.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/catalog/o_sys_cache.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef __O_SYS_CACHE_H__
#define __O_SYS_CACHE_H__

#include "orioledb.h"

#include "catalog/o_tables.h"
#include "catalog/sys_trees.h"
#include "miscadmin.h"
#include "recovery/recovery.h"

#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif

/*
 * Database oid to be used for sys cache entries comparison.
 */
extern Oid	o_sys_cache_cmp_datoid;

typedef uint32 OSysCacheHashKey;	/* GetSysCacheHashValue result type */

typedef struct OSysCache OSysCache;
typedef struct OSysCacheHashTreeEntry
{
	OSysCache *sys_cache;		/* If NULL only link stored */
	Pointer		entry;
	OSysCacheHashKey link;		/* stored hash of another entry of fastcache */
} OSysCacheHashTreeEntry;
typedef struct OSysCacheHashEntry
{
	OSysCacheHashKey key;
	List	   *tree_entries;	/* list of OSysCacheHashTreeEntry-s
								   that used because we store entries for all
								   sys caches in same fastcache for simpler
								   invalidation of dependent objects */
} OSysCacheHashEntry;

typedef struct OSysCacheFuncs
{
	/*
	 * Should be always set. Used in invalidation hook to cleanup entry saved
	 * in fastcache. Also used inside o_sys_cache_add_if_needed.
	 */
	void		(*free_entry) (Pointer entry);

	/*
	 * if set it called in o_sys_cache_delete before entry update with entry
	 * as argument
	 */
	void		(*delete_hook) (Pointer entry);

	/*
	 * Should be always set. Used inside o_sys_cache_add_if_needed and
	 * o_sys_cache_update_if_needed. On add entry_ptr is NULL, entry should
	 * be created and returned.
	 */
	void		(*fill_entry) (Pointer *entry_ptr, Oid datoid, Oid typoid,
							   XLogRecPtr insert_lsn, Pointer arg);

	/*
	 * if set, it called when a entry is created and must return oid of
	 * another object, invalidation of which shoud invalidate entry in
	 * fastcache
	 */
	Oid			(*fastcache_get_link_oid) (Pointer entry);

	/*
	 * Used in toast sys cache trees. Should return pointer to binary
	 * serialized data and it's length.
	 */
	Pointer		(*toast_serialize_entry) (Pointer entry, int *len);

	/*
	 * Used in toast sys cache trees. Should return pointer to constructed
	 * entry of a tree.
	 */
	Pointer		(*toast_deserialize_entry) (MemoryContext mcxt,
											Pointer data,
											Size length);
} OSysCacheFuncs;

typedef struct OSysCache
{
	int					sys_tree_num;
	bool				is_toast;
	bool				update_if_exist;
	Oid					classoid;
	MemoryContext		mcxt;	/* context where stored entries from fast
								 * cache */
	HTAB			   *fast_cache;		/* contains OSysCacheHashEntry-s */
	OSysCacheHashKey	last_fast_cache_key;
	Pointer				last_fast_cache_entry;
	OSysCacheFuncs	   *funcs;
} OSysCache;

/*
 * Initializes all sys catalog caches.
 */
extern void o_sys_caches_init(void);

extern OSysCache *o_create_sys_cache(int sys_tree_num, bool is_toast,
									   bool update_if_exist, Oid classoid,
									   HTAB *fast_cache, MemoryContext mcxt,
									   OSysCacheFuncs *funcs);
extern Pointer o_sys_cache_search(OSysCache *sys_cache, Oid datoid,
								   Oid oid, XLogRecPtr cur_lsn);
extern void o_sys_cache_add_if_needed(OSysCache *sys_cache, Oid datoid,
									   Oid oid, XLogRecPtr insert_lsn,
									   Pointer arg);
extern void o_sys_cache_update_if_needed(OSysCache *sys_cache,
										  Oid datoid, Oid oid, Pointer arg);
extern bool o_sys_cache_delete(OSysCache *sys_cache, Oid datoid, Oid oid);

extern void o_sys_cache_delete_by_lsn(OSysCache *sys_cache, XLogRecPtr lsn);

extern void custom_types_add_all(OTable *o_table, OTableIndex *o_table_index);
extern void custom_type_add_if_needed(Oid datoid, Oid typoid,
									  XLogRecPtr insert_lsn);

extern void orioledb_syscache_type_hook(Datum arg, int cacheid,
										uint32 hashvalue);

#define O_SYS_CACHE_INIT_FUNC(cache_name) \
void o_##cache_name##_init(MemoryContext mcxt, HTAB *fastcache)

#define O_SYS_CACHE_DECLS(cache_name, elem_type) \
extern O_SYS_CACHE_INIT_FUNC(cache_name); \
extern void o_##cache_name##_delete_by_lsn (XLogRecPtr lsn);		\
extern bool o_##cache_name##_delete(Oid datoid, Oid oid);			\
void o_##cache_name##_update_if_needed(Oid datoid, Oid oid,			\
									   Pointer arg);				\
void o_##cache_name##_add_if_needed(Oid datoid, Oid oid,			\
									XLogRecPtr insert_lsn,			\
									Pointer arg);					\
extern int no_such_variable

#define O_SYS_CACHE_FUNCS(cache_name, elem_type)					\
	static inline elem_type *o_##cache_name##_search(				\
		Oid datoid,	Oid oid, Oid lsn);								\
	elem_type *														\
	o_##cache_name##_search(Oid datoid, Oid oid, Oid lsn)			\
	{																\
		return (elem_type *)o_sys_cache_search(cache_name, datoid, \
												oid, lsn);			\
	}																\
	void															\
	o_##cache_name##_delete_by_lsn(XLogRecPtr lsn)					\
	{																\
		o_sys_cache_delete_by_lsn(cache_name, lsn);				\
	}																\
	bool															\
	o_##cache_name##_delete(Oid datoid, Oid oid)					\
	{																\
		return o_sys_cache_delete(cache_name, datoid, oid);		\
	}																\
	void															\
	o_##cache_name##_update_if_needed(Oid datoid, Oid oid,			\
									  Pointer arg)					\
	{																\
		o_sys_cache_update_if_needed(cache_name, datoid, oid,		\
									  arg);							\
	}																\
	void															\
	o_##cache_name##_add_if_needed(Oid datoid, Oid oid,				\
								   XLogRecPtr insert_lsn,			\
								   Pointer arg)						\
	{																\
		o_sys_cache_add_if_needed(cache_name, datoid, oid,			\
								   insert_lsn, arg);				\
	}																\
	extern int no_such_variable

static inline void
o_sys_cache_set_datoid_lsn(XLogRecPtr *cur_lsn, Oid *datoid)
{
	if (cur_lsn)
		*cur_lsn = is_recovery_in_progress() ? GetXLogReplayRecPtr(NULL) :
											   GetXLogWriteRecPtr();

	if (datoid)
	{
		if (OidIsValid(MyDatabaseId))
		{
			*datoid = MyDatabaseId;
		}
		else
		{
			Assert(OidIsValid(o_sys_cache_cmp_datoid));
			*datoid = o_sys_cache_cmp_datoid;
		}
	}
}

extern void o_composite_type_element_save(Oid datoid, Oid oid,
										  XLogRecPtr insert_lsn);

/* o_enum_cache.c */
typedef struct OEnum OEnum;

O_SYS_CACHE_DECLS(enum_cache, OEnum);
O_SYS_CACHE_DECLS(enumoid_cache, OEnumOid);
extern TypeCacheEntry *o_enum_cmp_internal_hook(Oid type_id,
												MemoryContext mcxt);

/* o_range_cache.c */
O_SYS_CACHE_DECLS(range_cache, ORange);
extern TypeCacheEntry *o_range_cmp_hook(FunctionCallInfo fcinfo,
										Oid rngtypid,
										MemoryContext mcxt);

/* o_class_cache.c */
typedef struct OClass OClass;

typedef struct OClassArg {
	bool	column_drop;
	bool	sys_table;
	int		dropped;
} OClassArg;

O_SYS_CACHE_DECLS(class_cache, OClass);
extern TupleDesc o_record_cmp_hook(Oid type_id, MemoryContext mcxt);
extern TupleDesc o_class_cache_search_tupdesc(Oid classoid);

/* o_type_element_cache.c */
O_SYS_CACHE_DECLS(type_element_cache, OTypeElement);
extern TypeCacheEntry *o_type_elements_cmp_hook(Oid elemtype,
												MemoryContext mcxt);

/* o_proc_cache.c */
typedef struct OProc OProc;

O_SYS_CACHE_DECLS(proc_cache, OProc);
extern Datum o_fmgr_sql(PG_FUNCTION_ARGS);
extern void o_proc_cache_validate_add(Oid datoid, Oid procoid, Oid fncollation,
									  char *func_type, char *used_for);

extern void o_proc_cache_fill_finfo(FmgrInfo *finfo, Oid procoid);

/* o_type_cache.c */
typedef struct OType OType;

O_SYS_CACHE_DECLS(type_cache, OType);
extern Oid o_type_cache_get_typrelid(Oid typeoid);
extern HeapTuple o_type_cache_search_htup(TupleDesc tupdesc, Oid typeoid);
extern void o_type_cache_fill_info(Oid typeoid, int16 *typlen, bool *typbyval,
								   char *typalign);

#endif							/* __O_SYS_CACHE_H__ */
