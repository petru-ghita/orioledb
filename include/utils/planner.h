/*-------------------------------------------------------------------------
 *
 * planner.h
 *		Routines for query processing.
 *
 * Copyright (c) 2021-2022, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/utils/planner.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __PLANNER_H__
#define __PLANNER_H__

extern Node *o_wrap_top_funcexpr(Node *node);
extern void o_process_sql_function(HeapTuple procedureTuple,
								   bool (*walker)(), void *context,
								   Oid functionId, Oid inputcollid,
								   List *args);
extern void o_process_functions_in_node(Node *node,
										void (*func_walker)(Oid functionId,
															Oid inputcollid,
															List *args,
															void *context),
										void *context);

#endif
