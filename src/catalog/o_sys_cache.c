/*-------------------------------------------------------------------------
 *
 * o_sys_cache.c
 *		Generic interface for sys cache duplicate trees.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_sys_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/btree.h"
#include "btree/modify.h"
#include "catalog/o_sys_cache.h"
#include "catalog/sys_trees.h"
#include "recovery/recovery.h"
#include "recovery/wal.h"
#include "transam/oxid.h"
#include "tuple/toast.h"
#include "utils/planner.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "common/hashfn.h"
#include "executor/functions.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/fmgrtab.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

Oid			o_sys_cache_cmp_datoid = InvalidOid;

static Pointer o_sys_cache_get_from_tree(OSysCache *sys_cache, Oid datoid,
										  Oid enumoid, XLogRecPtr cur_lsn);
static Pointer o_sys_cache_get_from_toast_tree(OSysCache *sys_cache,
												Oid datoid, Oid oid,
												XLogRecPtr cur_lsn);
static bool o_sys_cache_add(OSysCache *sys_cache, Oid datoid, Oid oid,
							 XLogRecPtr insert_lsn, Pointer entry);
static bool o_sys_cache_update(OSysCache *sys_cache, Pointer updated_entry);

static BTreeDescr *oSysCacheToastGetBTreeDesc(void *arg);
static uint32 oSysCacheToastGetMaxChunkSize(void *key, void *arg);
static void oSysCacheToastUpdateKey(void *key, uint32 offset, void *arg);
static void *oSysCacheToastGetNextKey(void *key, void *arg);
static OTuple oSysCacheToastCreateTuple(void *key, Pointer data,
										 uint32 offset, int length,
										 void *arg);
static OTuple oSysCacheToastCreateKey(void *key, uint32 offset, void *arg);
static Pointer oSysCacheToastGetTupleData(OTuple tuple, void *arg);
static uint32 oSysCacheToastGetTupleOffset(OTuple tuple, void *arg);
static uint32 oSysCacheToastGetTupleDataSize(OTuple tuple, void *arg);


ToastAPI	oSysCacheToastAPI = {
	.getBTreeDesc = oSysCacheToastGetBTreeDesc,
	.getMaxChunkSize = oSysCacheToastGetMaxChunkSize,
	.updateKey = oSysCacheToastUpdateKey,
	.getNextKey = oSysCacheToastGetNextKey,
	.createTuple = oSysCacheToastCreateTuple,
	.createKey = oSysCacheToastCreateKey,
	.getTupleData = oSysCacheToastGetTupleData,
	.getTupleOffset = oSysCacheToastGetTupleOffset,
	.getTupleDataSize = oSysCacheToastGetTupleDataSize,
	.deleteLogFullTuple = true,
	.versionCallback = NULL
};

static MemoryContext sys_cache_cxt = NULL;
static HTAB *sys_cache_fastcache;

/*
 * Initializes the enum B-tree memory.
 */
void
o_sys_caches_init(void)
{
	HASHCTL		ctl;

	sys_cache_cxt = AllocSetContextCreate(TopMemoryContext,
										 "OrioleDB sys_caches fastcache context",
										 ALLOCSET_DEFAULT_SIZES);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(OSysCacheHashKey);
	ctl.entrysize = sizeof(OSysCacheHashEntry);
	ctl.hcxt = sys_cache_cxt;
	sys_cache_fastcache = hash_create("OrioleDB sys_caches fastcache", 8,
									  &ctl,
									  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	o_enum_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_enumoid_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_range_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_type_element_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_class_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_proc_cache_init(sys_cache_cxt, sys_cache_fastcache);
	o_type_cache_init(sys_cache_cxt, sys_cache_fastcache);
}

/*
 * Initializes the enum B-tree memory.
 */
OSysCache *
o_create_sys_cache(int sys_tree_num, bool is_toast, bool update_if_exist,
					Oid classoid, HTAB *fast_cache,
					MemoryContext mcxt, OSysCacheFuncs *funcs)
{
	OSysCache *sys_cache;

	Assert(fast_cache);
	Assert(funcs);

	sys_cache = MemoryContextAllocZero(mcxt, sizeof(OSysCache));
	sys_cache->sys_tree_num = sys_tree_num;
	sys_cache->is_toast = is_toast;
	sys_cache->update_if_exist = update_if_exist;
	sys_cache->classoid = classoid;
	sys_cache->fast_cache = fast_cache;
	sys_cache->mcxt = mcxt;
	sys_cache->funcs = funcs;

#ifdef USE_ASSERT_CHECKING
	Assert(sys_cache->funcs->free_entry);
	Assert(sys_cache->funcs->fill_entry);
	if (sys_cache->is_toast)
	{
		Assert(sys_cache->funcs->toast_serialize_entry);
		Assert(sys_cache->funcs->toast_deserialize_entry);
	}
#endif

	return sys_cache;
}

/*
 * GetSysCacheHashValue for OID without SysCache usage
 */
static inline OSysCacheHashKey
o_sys_cache_GetSysCacheHashValue(Oid oid)
{
	uint32		hashValue = 0;
	uint32		oneHash;

	oneHash = murmurhash32(oid);

	hashValue ^= oneHash;

	return hashValue;
}

static void
invalidate_fastcache_entry(int cacheid, uint32 hashvalue)
{
	bool		found;
	OSysCacheHashEntry *fast_cache_entry;

	fast_cache_entry = (OSysCacheHashEntry *) hash_search(sys_cache_fastcache,
														  &hashvalue,
														  HASH_REMOVE,
														  &found);

	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OSysCacheHashTreeEntry *tree_entry;
			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache)
			{
				OSysCache *sys_cache = tree_entry->sys_cache;

				if (!memcmp(&sys_cache->last_fast_cache_key,
							&fast_cache_entry->key,
							sizeof(OSysCacheHashKey)))
				{
					memset(&sys_cache->last_fast_cache_key, 0,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = NULL;
				}
				tree_entry->sys_cache->funcs->free_entry(tree_entry->entry);
			}
			if (tree_entry->link != 0)
			{
				invalidate_fastcache_entry(cacheid, tree_entry->link);
			}
		}
		list_free_deep(fast_cache_entry->tree_entries);
	}
}

void
orioledb_syscache_type_hook(Datum arg, int cacheid, uint32 hashvalue)
{
	if (sys_cache_fastcache)
		invalidate_fastcache_entry(cacheid, hashvalue);
}

Pointer
o_sys_cache_search(OSysCache *sys_cache, Oid datoid,
					Oid oid, XLogRecPtr cur_lsn)
{
	bool						found;
	OSysCacheHashKey			cur_fast_cache_key;
	OSysCacheHashEntry		   *fast_cache_entry;
	Pointer						tree_entry;
	MemoryContext				prev_context;
	OSysCacheHashTreeEntry	   *new_entry;

	cur_fast_cache_key = o_sys_cache_GetSysCacheHashValue(oid);

	/* fast search */
	if (!memcmp(&cur_fast_cache_key, &sys_cache->last_fast_cache_key,
				sizeof(OSysCacheHashKey)) &&
		sys_cache->last_fast_cache_entry)
	{
		OSysCacheKey   *sys_cache_key;
		sys_cache_key = (OSysCacheKey *) sys_cache->last_fast_cache_entry;

		if (sys_cache_key->datoid == datoid && sys_cache_key->oid == oid)
			return sys_cache->last_fast_cache_entry;
	}

	/* cache search */
	fast_cache_entry = (OSysCacheHashEntry *)
		hash_search(sys_cache->fast_cache, &cur_fast_cache_key, HASH_ENTER,
					&found);
	if (found)
	{
		ListCell   *lc;

		foreach(lc, fast_cache_entry->tree_entries)
		{
			OSysCacheHashTreeEntry *tree_entry;
			tree_entry = (OSysCacheHashTreeEntry *) lfirst(lc);

			if (tree_entry->sys_cache == sys_cache)
			{
				OSysCacheKey   *sys_cache_key;
				sys_cache_key = (OSysCacheKey *) tree_entry->entry;

				if (sys_cache_key->datoid == datoid &&
					sys_cache_key->oid == oid)
				{
					memcpy(&sys_cache->last_fast_cache_key,
						   &cur_fast_cache_key,
						   sizeof(OSysCacheHashKey));
					sys_cache->last_fast_cache_entry = tree_entry->entry;
					return sys_cache->last_fast_cache_entry;
				}
			}
		}
	}
	else
		fast_cache_entry->tree_entries = NIL;

	prev_context = MemoryContextSwitchTo(sys_cache->mcxt);
	if (sys_cache->is_toast)
		tree_entry = o_sys_cache_get_from_toast_tree(sys_cache, datoid,
													  oid, cur_lsn);
	else
		tree_entry = o_sys_cache_get_from_tree(sys_cache, datoid,
												oid, cur_lsn);
	if (tree_entry == NULL)
	{
		MemoryContextSwitchTo(prev_context);
		return NULL;
	}
	new_entry = palloc0(sizeof(OSysCacheHashTreeEntry));
	new_entry->sys_cache = sys_cache;
	new_entry->entry = tree_entry;
	if (sys_cache->funcs->fastcache_get_link_oid)
	{
		Oid			link_oid = sys_cache->funcs->fastcache_get_link_oid(tree_entry);
		OSysCacheHashKey link_key = o_sys_cache_GetSysCacheHashValue(link_oid);
		OSysCacheHashTreeEntry *link_entry;
		OSysCacheHashEntry *link_cache_entry;

		link_entry = palloc0(sizeof(OSysCacheHashTreeEntry));
		link_entry->sys_cache = NULL;
		link_entry->link = cur_fast_cache_key;
		new_entry->link = link_key;
		memcpy(&new_entry->link, &link_key, sizeof(OSysCacheHashKey));

		link_cache_entry = (OSysCacheHashEntry *)
			hash_search(sys_cache->fast_cache, &link_key, HASH_ENTER, &found);
		if (!found)
			link_cache_entry->tree_entries = NIL;
		link_cache_entry->tree_entries = lappend(link_cache_entry->tree_entries,
												 link_entry);
	}

	fast_cache_entry->tree_entries = lappend(fast_cache_entry->tree_entries,
											 new_entry);

	MemoryContextSwitchTo(prev_context);

	memcpy(&sys_cache->last_fast_cache_key,
		   &cur_fast_cache_key,
		   sizeof(OSysCacheHashKey));
	sys_cache->last_fast_cache_entry = new_entry->entry;
	return sys_cache->last_fast_cache_entry;
}

static TupleFetchCallbackResult
o_sys_cache_get_by_lsn_callback(OTuple tuple, OXid tupOxid, CommitSeqNo csn,
								 void *arg,
								 TupleFetchCallbackCheckType check_type)
{
	OSysCacheToastChunkKey *tuple_key = (OSysCacheToastChunkKey *) tuple.data;
	XLogRecPtr *cur_lsn = (XLogRecPtr *) arg;

	if (check_type != OTupleFetchCallbackKeyCheck)
		return OTupleFetchNext;

	if (tuple_key->sys_cache_key.insert_lsn < *cur_lsn)
		return OTupleFetchMatch;
	else
		return OTupleFetchNext;
}

Pointer
o_sys_cache_get_from_toast_tree(OSysCache *sys_cache, Oid datoid,
								 Oid oid, XLogRecPtr cur_lsn)
{
	Pointer		data;
	Size		dataLength;
	Pointer		result = NULL;
	BTreeDescr *td = get_sys_tree(sys_cache->sys_tree_num);
	OSysCacheToastKeyBound toast_key = {
		.chunk_key = {.sys_cache_key = {.datoid = datoid,
										.oid = oid,
										.insert_lsn = cur_lsn},
					  .offset = 0},
		.lsn_cmp = false
	};

	data = generic_toast_get_any_with_callback(&oSysCacheToastAPI,
											   (Pointer) &toast_key,
											   &dataLength,
											   COMMITSEQNO_NON_DELETED,
											   td,
											   o_sys_cache_get_by_lsn_callback,
											   &cur_lsn);
	if (data == NULL)
		return NULL;
	result = sys_cache->funcs->toast_deserialize_entry(sys_cache->mcxt,
														data, dataLength);
	pfree(data);

	return result;
}

Pointer
o_sys_cache_get_from_tree(OSysCache *sys_cache, Oid datoid,
						   Oid oid, XLogRecPtr cur_lsn)
{
	BTreeDescr		   *td = get_sys_tree(sys_cache->sys_tree_num);
	BTreeIterator	   *it;
	OTuple				last_tup;
	OSysCacheKeyBound	key;

	key.datoid = datoid;
	key.oid = oid;

	it = o_btree_iterator_create(td, (Pointer) &key, BTreeKeyBound,
								 COMMITSEQNO_INPROGRESS, ForwardScanDirection);

	O_TUPLE_SET_NULL(last_tup);
	do
	{
		OTuple				tup = o_btree_iterator_fetch(it, NULL,
														 (Pointer) &key,
														 BTreeKeyBound, true,
														 NULL);
		OSysCacheKey   *sys_cache_key;

		if (O_TUPLE_IS_NULL(tup))
			break;

		if (!O_TUPLE_IS_NULL(last_tup))
			pfree(last_tup.data);

		sys_cache_key = (OSysCacheKey *) tup.data;
		if (sys_cache_key->insert_lsn > cur_lsn)
			break;
		last_tup = tup;
	} while (true);

	btree_iterator_free(it);

	return last_tup.data;
}

static inline void
o_sys_cache_fill_locktag(LOCKTAG *tag, Oid datoid, Oid oid, Oid classoid,
						  int lockmode)
{
	Assert(lockmode == AccessShareLock || lockmode == AccessExclusiveLock);
	memset(tag, 0, sizeof(LOCKTAG));
	SET_LOCKTAG_OBJECT(*tag, datoid, classoid, oid, 0);
}

static void
o_sys_cache_lock(Oid datoid, Oid oid, Oid classoid, int lockmode)
{
	LOCKTAG		locktag;

	o_sys_cache_fill_locktag(&locktag, datoid, oid, classoid, lockmode);

	LockAcquire(&locktag, lockmode, false, false);
}

static void
o_sys_cache_unlock(Oid datoid, Oid oid, Oid classoid, int lockmode)
{
	LOCKTAG		locktag;

	o_sys_cache_fill_locktag(&locktag, datoid, oid, classoid, lockmode);

	if (!LockRelease(&locktag, lockmode, false))
	{
		elog(ERROR, "Can not release %s catalog cache lock on datoid = %d, "
			 "oid = %d",
			 lockmode == AccessShareLock ? "share" : "exclusive",
			 datoid, oid);
	}
}

/* Non-key fields of entry should be filled before call */
bool
o_sys_cache_add(OSysCache *sys_cache, Oid datoid, Oid oid,
				 XLogRecPtr insert_lsn, Pointer entry)
{
	bool		inserted;
	OSysCacheKey   *sys_cache_key = (OSysCacheKey *) entry;
	BTreeDescr *desc = get_sys_tree(sys_cache->sys_tree_num);

	sys_cache_key->datoid = datoid;
	sys_cache_key->oid = oid;
	sys_cache_key->insert_lsn = insert_lsn;
	sys_cache_key->deleted = false;

	if (!sys_cache->is_toast)
	{
		OTuple		tup;

		tup.formatFlags = 0;
		tup.data = entry;
		inserted = o_btree_autonomous_insert(desc, tup);
	}
	else
	{
		Pointer		data;
		int			len;
		OSysCacheToastKeyBound toast_key = {0};
		OAutonomousTxState state;

		toast_key.chunk_key.sys_cache_key = *sys_cache_key;
		toast_key.chunk_key.offset = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			inserted = generic_toast_insert(&oSysCacheToastAPI,
											(Pointer) &toast_key,
											data, len,
											get_current_oxid(),
											COMMITSEQNO_INPROGRESS,
											desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
		pfree(data);
	}
	return inserted;
}

static OBTreeWaitCallbackAction
o_sys_cache_wait_callback(BTreeDescr *descr,
						   OTuple tup, OTuple *newtup, OXid oxid, OTupleXactInfo xactInfo, UndoLocation location,
						   RowLockMode *lock_mode, BTreeLocationHint *hint,
						   void *arg)
{
	return OBTreeCallbackActionXidWait;
}

static OBTreeModifyCallbackAction
o_sys_cache_update_callback(BTreeDescr *descr,
							 OTuple tup, OTuple *newtup, OXid oxid, OTupleXactInfo xactInfo, UndoLocation location,
							 RowLockMode *lock_mode, BTreeLocationHint *hint,
							 void *arg)
{
	return OBTreeCallbackActionUpdate;
}


static BTreeModifyCallbackInfo callbackInfo =
{
	.waitCallback = o_sys_cache_wait_callback,
	.modifyCallback = o_sys_cache_update_callback,
	.modifyDeletedCallback = o_sys_cache_update_callback,
	.arg = NULL
};

bool
o_sys_cache_update(OSysCache *sys_cache, Pointer updated_entry)
{
	bool				result;
	OSysCacheKeyBound	key;
	OSysCacheKey	   *sys_cache_key;
	BTreeDescr		   *desc = get_sys_tree(sys_cache->sys_tree_num);

	sys_cache_key = (OSysCacheKey *) updated_entry;

	key.datoid = sys_cache_key->datoid;
	key.oid = sys_cache_key->oid;

	if (!sys_cache->is_toast)
	{
		OAutonomousTxState state;
		OTuple		tup;

		tup.formatFlags = 0;
		tup.data = updated_entry;

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = o_btree_modify(desc, BTreeOperationUpdate,
									tup, BTreeKeyLeafTuple,
									(Pointer) &key, BTreeKeyBound,
									get_current_oxid(), COMMITSEQNO_INPROGRESS,
									RowLockNoKeyUpdate, NULL,
									&callbackInfo) == OBTreeModifyResultUpdated;
			if (result)
				o_wal_update(desc, tup);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	else
	{
		Pointer					data;
		int						len;
		OSysCacheToastKeyBound	toast_key = {0};
		OAutonomousTxState		state;

		toast_key.chunk_key.sys_cache_key = *sys_cache_key;
		toast_key.chunk_key.offset = 0;
		toast_key.lsn_cmp = true;

		data = sys_cache->funcs->toast_serialize_entry(updated_entry, &len);

		start_autonomous_transaction(&state);
		PG_TRY();
		{
			result = generic_toast_update(&oSysCacheToastAPI,
										  (Pointer) &toast_key,
										  data, len,
										  get_current_oxid(),
										  COMMITSEQNO_INPROGRESS,
										  desc);
		}
		PG_CATCH();
		{
			abort_autonomous_transaction(&state);
			PG_RE_THROW();
		}
		PG_END_TRY();
		finish_autonomous_transaction(&state);
	}
	return result;
}

void
o_sys_cache_add_if_needed(OSysCache *sys_cache, Oid datoid,
						   Oid oid, XLogRecPtr insert_lsn, Pointer arg)
{
	Pointer		entry = NULL;
	bool		inserted PG_USED_FOR_ASSERTS_ONLY;

	o_sys_cache_lock(datoid, oid, sys_cache->classoid,
					  AccessExclusiveLock);

	entry = o_sys_cache_search(sys_cache, datoid, oid, insert_lsn);
	if (entry != NULL)
	{
		/* it's already exist in B-tree */
		if (sys_cache->update_if_exist)
			o_sys_cache_update_if_needed(sys_cache, datoid, oid, arg);
		return;
	}

	sys_cache->funcs->fill_entry(&entry, datoid, oid, insert_lsn, arg);

	Assert(entry);

	/*
	 * All done, now try to insert into B-tree.
	 */
	inserted = o_sys_cache_add(sys_cache, datoid, oid,
								insert_lsn, entry);
	Assert(inserted);
	o_sys_cache_unlock(datoid, oid, sys_cache->classoid,
						AccessExclusiveLock);
	sys_cache->funcs->free_entry(entry);
}

void
o_sys_cache_update_if_needed(OSysCache *sys_cache,
							  Oid datoid, Oid oid, Pointer arg)
{
	Pointer			entry = NULL;
	XLogRecPtr		cur_lsn;
	OSysCacheKey   *sys_cache_key;
	bool			updated PG_USED_FOR_ASSERTS_ONLY;

	o_sys_cache_lock(datoid, oid, sys_cache->classoid,
					  AccessExclusiveLock);

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	entry = o_sys_cache_search(sys_cache, datoid, oid, cur_lsn);
	if (entry == NULL)
	{
		/* it's not exist in B-tree */
		return;
	}

	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache->funcs->fill_entry(&entry, datoid, oid,
								 sys_cache_key->insert_lsn, arg);

	updated = o_sys_cache_update(sys_cache, entry);
	Assert(updated);
	o_sys_cache_unlock(datoid, oid, sys_cache->classoid,
						AccessExclusiveLock);
}

bool
o_sys_cache_delete(OSysCache *sys_cache, Oid datoid, Oid oid)
{
	Pointer			entry;
	OSysCacheKey   *sys_cache_key;

	entry = o_sys_cache_search(sys_cache, datoid, oid,
								GetXLogWriteRecPtr());

	if (entry == NULL)
		return false;

	if (sys_cache->funcs->delete_hook)
		sys_cache->funcs->delete_hook(entry);

	sys_cache_key = (OSysCacheKey *) entry;
	sys_cache_key->deleted = true;

	return o_sys_cache_update(sys_cache, entry);
}

void
o_sys_cache_delete_by_lsn(OSysCache *sys_cache, XLogRecPtr lsn)
{
	BTreeIterator *it;
	BTreeDescr *td = get_sys_tree(sys_cache->sys_tree_num);

	it = o_btree_iterator_create(td, NULL, BTreeKeyNone,
								 COMMITSEQNO_NON_DELETED, ForwardScanDirection);

	do
	{
		bool		end;
		BTreeLocationHint hint;
		OTuple		tup = btree_iterate_raw(it, NULL, BTreeKeyNone,
											false, &end, &hint);
		OSysCacheKey   *sys_cache_key;
		OTuple			key_tup;

		if (O_TUPLE_IS_NULL(tup))
		{
			if (end)
				break;
			else
				continue;
		}

		sys_cache_key = (OSysCacheKey *) tup.data;
		key_tup.formatFlags = 0;
		key_tup.data = (Pointer) sys_cache_key;

		if (sys_cache_key->insert_lsn < lsn && sys_cache_key->deleted)
		{
			bool		result PG_USED_FOR_ASSERTS_ONLY;

			if (!sys_cache->is_toast)
			{
				result = o_btree_autonomous_delete(td, key_tup, BTreeKeyNonLeafKey, &hint);
			}
			else
			{
				OSysCacheToastKeyBound	toast_key = {0};
				OAutonomousTxState		state;

				toast_key.chunk_key.sys_cache_key = *sys_cache_key;
				toast_key.chunk_key.offset = 0;
				toast_key.lsn_cmp = true;

				start_autonomous_transaction(&state);
				PG_TRY();
				{
					result = generic_toast_delete(&oSysCacheToastAPI,
												  (Pointer) &toast_key,
												  get_current_oxid(),
												  COMMITSEQNO_NON_DELETED,
												  td);
				}
				PG_CATCH();
				{
					abort_autonomous_transaction(&state);
					PG_RE_THROW();
				}
				PG_END_TRY();
				finish_autonomous_transaction(&state);
			}

			Assert(result);
		}
	} while (true);

	btree_iterator_free(it);
}


static BTreeDescr *
oSysCacheToastGetBTreeDesc(void *arg)
{
	BTreeDescr *desc = (BTreeDescr *) arg;

	return desc;
}

static uint32
oSysCacheToastGetMaxChunkSize(void *key, void *arg)
{
	uint32		max_chunk_size;

	max_chunk_size =
		MAXALIGN_DOWN((O_BTREE_MAX_TUPLE_SIZE * 3 -
					   MAXALIGN(sizeof(OSysCacheToastChunkKey))) / 3) -
		offsetof(OSysCacheToastChunk, data);

	return max_chunk_size;
}

static void
oSysCacheToastUpdateKey(void *key, uint32 offset, void *arg)
{
	OSysCacheToastKeyBound *ckey = (OSysCacheToastKeyBound *) key;

	ckey->chunk_key.offset = offset;
}

static void *
oSysCacheToastGetNextKey(void *key, void *arg)
{
	OSysCacheToastKeyBound		   *ckey = (OSysCacheToastKeyBound *) key;
	static OSysCacheToastKeyBound	nextKey;

	nextKey = *ckey;
	nextKey.chunk_key.sys_cache_key.oid++;
	nextKey.chunk_key.offset = 0;

	return (Pointer) &nextKey;
}

static OTuple
oSysCacheToastCreateTuple(void *key, Pointer data, uint32 offset,
						   int length, void *arg)
{
	OSysCacheToastKeyBound	   *bound = (OSysCacheToastKeyBound *) key;
	OSysCacheToastChunk		   *chunk;
	OTuple						result;

	bound->chunk_key.offset = offset;

	chunk = (OSysCacheToastChunk *)
		palloc0(offsetof(OSysCacheToastChunk, data) + length);
	chunk->key = bound->chunk_key;
	chunk->dataLength = length;
	memcpy(chunk->data, data + offset, length);

	result.data = (Pointer) chunk;
	result.formatFlags = 0;

	return result;
}

static OTuple
oSysCacheToastCreateKey(void *key, uint32 offset, void *arg)
{
	OSysCacheToastChunkKey	   *ckey = (OSysCacheToastChunkKey *) key;
	OSysCacheToastChunkKey	   *ckey_copy;
	OTuple						result;

	ckey_copy = (OSysCacheToastChunkKey *) palloc(sizeof(OSysCacheToastChunkKey));
	*ckey_copy = *ckey;

	result.data = (Pointer) ckey_copy;
	result.formatFlags = 0;

	return result;
}

static Pointer
oSysCacheToastGetTupleData(OTuple tuple, void *arg)
{
	OSysCacheToastChunk *chunk = (OSysCacheToastChunk *) tuple.data;

	return chunk->data;
}

static uint32
oSysCacheToastGetTupleOffset(OTuple tuple, void *arg)
{
	OSysCacheToastChunk *chunk = (OSysCacheToastChunk *) tuple.data;

	return chunk->key.offset;
}

static uint32
oSysCacheToastGetTupleDataSize(OTuple tuple, void *arg)
{
	OSysCacheToastChunk *chunk = (OSysCacheToastChunk *) tuple.data;

	return chunk->dataLength;
}

void
custom_type_add_if_needed(Oid datoid, Oid typoid, XLogRecPtr insert_lsn)
{
	Form_pg_type typeform;
	HeapTuple	tuple = NULL;

	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typoid));
	Assert(tuple);
	typeform = (Form_pg_type) GETSTRUCT(tuple);

	switch (typeform->typtype)
	{
		case TYPTYPE_COMPOSITE:
			if (typeform->typtypmod == -1)
			{
				OClassArg	arg = {.sys_table=false};
				o_class_cache_add_if_needed(datoid, typeform->typrelid,
											insert_lsn, (Pointer) &arg);
				o_type_cache_add_if_needed(datoid, typeform->oid,
										   insert_lsn, NULL);
			}
			break;
		case TYPTYPE_RANGE:
			o_range_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
										NULL);
			break;
		case TYPTYPE_ENUM:
			o_enum_cache_add_if_needed(datoid, typeform->oid, insert_lsn,
									   NULL);
			break;
		case TYPTYPE_DOMAIN:
			custom_type_add_if_needed(datoid, typeform->typbasetype,
									  insert_lsn);
			break;
		default:
			if (typeform->typcategory == TYPCATEGORY_ARRAY)
			{
				o_composite_type_element_save(datoid, typeform->typelem,
											  insert_lsn);
			}
			break;
	}
	if (tuple != NULL)
		ReleaseSysCache(tuple);
}

/*
 * Inserts type elements for all fields of the o_table to the orioledb sys
 * cache.
 */
void
custom_types_add_all(OTable *o_table, OTableIndex *o_table_index)
{
	int			cur_field;
	XLogRecPtr	cur_lsn;
	int			expr_field = 0;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	for (cur_field = 0; cur_field < o_table_index->nfields; cur_field++)
	{
		int	attnum = o_table_index->fields[cur_field].attnum;
		Oid	typid;

		if (attnum != EXPR_ATTNUM)
			typid = o_table->fields[attnum].typid;
		else
			typid = o_table_index->exprfields[expr_field++].typid;
		custom_type_add_if_needed(o_table->oids.datoid, typid, cur_lsn);
	}
}

void
o_composite_type_element_save(Oid datoid, Oid oid, XLogRecPtr insert_lsn)
{
	o_type_element_cache_add_if_needed(datoid, oid, insert_lsn, NULL);
	custom_type_add_if_needed(datoid, oid, insert_lsn);
}
