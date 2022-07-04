/*-------------------------------------------------------------------------
 *
 *  o_type_cache.c
 *		Routines for orioledb type cache.
 *
 * type_cache is tree that contains cached metadata from pg_type.
 *
 * Copyright (c) 2021-2022, OrioleDB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_type_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"
#include "recovery/recovery.h"

#include "catalog/pg_type.h"
#include "miscadmin.h"
#include "utils/catcache.h"
#include "utils/syscache.h"

static OSysCache *type_cache = NULL;

/* TODO: Remove type_element_cache usage and replace it with type_cache usage */
/* TODO: Remove jf_atttype usage for proc_cache and replace it with type_cache
   usage
*/
struct OType
{
	OSysCacheKey	key;
	Oid				oid;
	NameData		typeName;
	int16			typlen;
	bool			typbyval;
	char			typalign;
	char			typstorage;
	Oid				typcollation;
	Oid				typrelid;
};

static void o_type_cache_fill_entry(Pointer *entry_ptr, Oid datoid,
									Oid typeoid, XLogRecPtr insert_lsn,
									Pointer arg);
static void o_type_cache_free_entry(Pointer entry);
static Pointer o_type_cache_serialize_entry(Pointer entry, int *len);
static Pointer o_type_cache_deserialize_entry(MemoryContext mcxt, Pointer data,
											  Size length);
O_SYS_CACHE_FUNCS(type_cache, OType);

static OSysCacheFuncs type_cache_funcs =
{
	.free_entry = o_type_cache_free_entry,
	.fill_entry = o_type_cache_fill_entry,
	.toast_serialize_entry = o_type_cache_serialize_entry,
	.toast_deserialize_entry = o_type_cache_deserialize_entry,
};

/*
 * Initializes the type sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(type_cache)
{
	type_cache = o_create_sys_cache(SYS_TREES_TYPE_CACHE,
									true, false,
									TypeRelationId,
									fastcache,
									mcxt,
									&type_cache_funcs);
}


void
o_type_cache_fill_entry(Pointer *entry_ptr, Oid datoid, Oid typeoid,
			  			XLogRecPtr insert_lsn, Pointer arg)
{
	HeapTuple		typetup;
	Form_pg_type	typeform;
	OType		   *o_type = (OType *) *entry_ptr;

	typetup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typeoid));
	if (!HeapTupleIsValid(typetup))
		elog(ERROR, "cache lookup failed for type %u", typeoid);
	typeform = (Form_pg_type) GETSTRUCT(typetup);

	if (o_type != NULL)		/* Existed o_type updated */
	{
		Assert(false);
	}
	else
	{
		o_type = palloc0(sizeof(OType));
		*entry_ptr = (Pointer) o_type;
	}
	// Copy forming of tuple from TypeCreate
	o_type->oid = typeform->oid;
	o_type->typeName = typeform->typname;
	o_type->typlen = typeform->typlen;
	o_type->typbyval = typeform->typbyval;
	o_type->typalign = typeform->typalign;
	o_type->typstorage = typeform->typstorage;
	o_type->typcollation = typeform->typcollation;
	o_type->typrelid = typeform->typrelid;
	ReleaseSysCache(typetup);
}

void
o_type_cache_free_entry(Pointer entry)
{
	OType	   *o_type = (OType *) entry;

	pfree(o_type);
}

Pointer
o_type_cache_serialize_entry(Pointer entry, int *len)
{
	StringInfoData	str;
	OType		   *o_type = (OType *) entry;

	initStringInfo(&str);
	appendBinaryStringInfo(&str, (Pointer) o_type, sizeof(OType));

	*len = str.len;
	return str.data;
}

Pointer
o_type_cache_deserialize_entry(MemoryContext mcxt, Pointer data, Size length)
{
	Pointer			ptr = data;
	OType		   *o_type;
	int				len;

	o_type = (OType *) palloc(sizeof(OType));
	len = sizeof(OType);
	Assert((ptr - data) + len <= length);
	memcpy(o_type, ptr, len);
	ptr += len;

	return (Pointer) o_type;
}

HeapTuple
o_type_cache_search_htup(TupleDesc tupdesc, Oid typeoid)
{
	XLogRecPtr		cur_lsn;
	Oid				datoid;
	HeapTuple		typetup = NULL;
	Datum			values[Natts_pg_type] = {0};
	bool			nulls[Natts_pg_type];
	OType		   *o_type;

	memset(nulls, true, sizeof(nulls));

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn);
	if (o_type)
	{
		// Copy forming of tuple from TypeCreate
		values[Anum_pg_type_oid - 1] = ObjectIdGetDatum(&o_type->oid);
		nulls[Anum_pg_type_oid - 1] = false;
		values[Anum_pg_type_typname - 1] = NameGetDatum(&o_type->typeName);
		nulls[Anum_pg_type_typname - 1] = false;
		values[Anum_pg_type_typlen - 1] = Int16GetDatum(o_type->typlen);
		nulls[Anum_pg_type_typlen - 1] = false;
		values[Anum_pg_type_typbyval - 1] = BoolGetDatum(o_type->typbyval);
		nulls[Anum_pg_type_typbyval - 1] = false;
		values[Anum_pg_type_typalign - 1] = CharGetDatum(o_type->typalign);
		nulls[Anum_pg_type_typalign - 1] = false;
		values[Anum_pg_type_typstorage - 1] = CharGetDatum(o_type->typstorage);
		nulls[Anum_pg_type_typstorage - 1] = false;
		values[Anum_pg_type_typcollation - 1] = ObjectIdGetDatum(o_type->typcollation);
		nulls[Anum_pg_type_typcollation - 1] = false;
		values[Anum_pg_type_typrelid - 1] = ObjectIdGetDatum(o_type->typrelid);
		nulls[Anum_pg_type_typrelid - 1] = false;

		typetup = heap_form_tuple(tupdesc, values, nulls);
	}
	return typetup;
}

Oid
o_type_cache_get_typrelid(Oid typeoid)
{
	XLogRecPtr		cur_lsn;
	Oid				datoid;
	OType		   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn);
	Assert(o_type);
	return o_type->typrelid;
}

void
o_type_cache_fill_info(Oid typeoid, int16 *typlen, bool *typbyval,
					   char *typalign)
{
	XLogRecPtr		cur_lsn;
	Oid				datoid;
	OType		   *o_type;

	o_sys_cache_set_datoid_lsn(&cur_lsn, &datoid);
	o_type = o_type_cache_search(datoid, typeoid, cur_lsn);

	Assert(o_type);
	*typlen = o_type->typlen;
	*typbyval = o_type->typbyval;
	*typalign = o_type->typalign;
}
