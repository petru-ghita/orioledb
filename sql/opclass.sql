CREATE EXTENSION orioledb;

CREATE SCHEMA my_funcs;
-- TODO: Remove unused functions when test done
CREATE TYPE my_funcs.my_enum AS ENUM ('a', 'b', 'c', 'd', 'e', 'f');

CREATE TYPE my_funcs.my_record AS (
	val_1 integer,
	val_2 my_funcs.my_enum
);

CREATE TABLE o_test_custom_opclass (
	int_val int,
	rec_val my_funcs.my_record
) USING orioledb;

CREATE FUNCTION my_funcs.my_int_cmp_plpgsql(a int, b int) RETURNS int
AS $$
DECLARE
	a_tr int := (a::bit(5) & X'A8'::bit(5))::int;
	b_tr int := (b::bit(5) & X'A8'::bit(5))::int;
BEGIN
	RETURN btint4cmp(a_tr, b_tr);
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_funcs.my_int_cmp_s_p(a int, b int) RETURNS int
	LANGUAGE SQL RETURN my_funcs.my_int_cmp_plpgsql(a, b);

CREATE FUNCTION my_funcs.my_int_cmp_s_s_p(a int, b int) RETURNS int
	LANGUAGE SQL RETURN my_funcs.my_int_cmp_s_p(a, b);

CREATE FUNCTION my_funcs.my_int_cmp_s_s_s_p(a int, b int) RETURNS int
	LANGUAGE SQL RETURN my_funcs.my_int_cmp_s_s_p(a, b);

CREATE FUNCTION my_funcs.my_int_cmp_sql(a int, b int) RETURNS int
AS $$
	SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int, 
					 (b::bit(5) & X'A8'::bit(5))::int);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_funcs.my_int_cmp_s_s(a int, b int) RETURNS int
	LANGUAGE SQL IMMUTABLE RETURN my_funcs.my_int_cmp_sql(a, b);

CREATE FUNCTION my_funcs.my_int_cmp_s_s_s(a int, b int) RETURNS int
	LANGUAGE SQL IMMUTABLE RETURN my_funcs.my_int_cmp_s_s(a, b);

CREATE FUNCTION my_funcs.my_int_cmp_s_s_s_s(a int, b int) RETURNS int
	LANGUAGE SQL IMMUTABLE RETURN my_funcs.my_int_cmp_s_s_s(a, b);

CREATE FUNCTION my_funcs.my_int_eq(a int, b int) RETURNS bool AS $$
	SELECT my_funcs.my_int_cmp_sql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_funcs.my_int_eq_plpgsql(a int, b int)
	RETURNS bool
AS $$
	SELECT my_funcs.my_int_cmp_plpgsql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

-- TODO: Remove
CREATE TABLE test_ints AS (SELECT generate_series(1, 5) val);

CREATE PROCEDURE my_funcs.my_test_proc(a int, b int) AS $$
	SELECT a + b + 3;
$$ LANGUAGE SQL;

CREATE FUNCTION my_funcs.my_test_func(a int, b int) RETURNS int AS $$
	SELECT GREATEST(a, b) - LEAST(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_funcs.my_complex_cmp_sql(a int, b int) RETURNS int
AS $$
	-- CALL my_funcs.my_test_proc(1, 5);
	-- SELECT val FROM test_ints LIMIT 1;
	-- SELECT * FROM regression.public.test_ints LIMIT 1;
	-- SELECT (VALUES (1), (3), (5)) s1 LIMIT 1;
	-- SELECT int_val FROM o_test_custom_opclass LIMIT 1;
	SELECT my_funcs.my_int_cmp_sql(a, b);
	-- SELECT 1 + 3 - a - b;
	-- SELECT COUNT(*) % 2 FROM generate_series(LEAST(a, b), GREATEST(a, b));
	-- SELECT s1 + a - b FROM (SELECT 1) s1;
	-- SELECT * FROM ROWS FROM(my_funcs.my_test_func(a, b));
	-- SELECT * FROM (VALUES (1), (3), (5)) s1 LIMIT 1;
	-- SELECT (VALUES (1)) s1;
	-- SELECT * FROM (VALUES (1)) s1 LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
	-- WITH a1 AS (
	--     SELECT generate_series(LEAST(1, 5), GREATEST(1, 5))
	-- )
	-- SELECT * FROM a1 LIMIT 1;
	-- SELECT * FROM (SELECT 2) s1;
	-- SELECT -2;
$$ LANGUAGE SQL IMMUTABLE;

CREATE OPERATOR my_funcs.=^ (
 LEFTARG = int4,
 RIGHTARG = int4,
 PROCEDURE = my_funcs.my_int_eq_plpgsql,
 COMMUTATOR = OPERATOR(my_funcs.=^)
);
CREATE OPERATOR my_funcs.<^ (
 LEFTARG = int4,
 RIGHTARG = int4,
 PROCEDURE = int4lt
);
CREATE OPERATOR my_funcs.>^ (
 LEFTARG = int4,
 RIGHTARG = int4,
 PROCEDURE = int4gt
);
CREATE OPERATOR FAMILY my_funcs.my_op_family USING btree;
-- CREATE OPERATOR CLASS my_funcs.my_op_class FOR TYPE int 
-- 	USING btree FAMILY my_funcs.my_op_family 
-- 	AS OPERATOR 1 my_funcs.<^, OPERATOR 3 my_funcs.=^, OPERATOR 5 my_funcs.>^,
-- 	   FUNCTION 1 my_funcs.my_int_cmp_s_s_s_p(int, int);

-- CREATE INDEX int_val_ix ON o_test_custom_opclass(int_val my_funcs.my_op_class);

-- DROP OPERATOR CLASS my_funcs.my_op_class USING btree;

CREATE OPERATOR CLASS my_funcs.my_op_class FOR TYPE int 
	USING btree FAMILY my_funcs.my_op_family 
	AS OPERATOR 1 my_funcs.<^, OPERATOR 3 my_funcs.=^, OPERATOR 5 my_funcs.>^,
	   FUNCTION 1 my_funcs.my_complex_cmp_sql(int, int);
	--    FUNCTION 1 my_funcs.my_int_cmp_sql(int, int);

CREATE INDEX int_val_ix ON o_test_custom_opclass(int_val my_funcs.my_op_class);

EXPLAIN (ANALYZE, VERBOSE) SELECT a, my_funcs.my_complex_cmp_sql(a, a + 10)
	FROM generate_series(1, 10) a;

SET log_min_messages = 'debug4';
SET client_min_messages = 'debug4';

-- EXPLAIN (ANALYZE, VERBOSE) 
INSERT INTO o_test_custom_opclass
	SELECT val, (val, 
				(enum_range(NULL::my_funcs.my_enum))[val]
				)::my_funcs.my_record
		FROM generate_series(1,3) val;
-- TRUNCATE test_ints;
-- INSERT INTO o_test_custom_opclass
--     SELECT val, (val, 
--                 (enum_range(NULL::my_funcs.my_enum))[val]
--                 )::my_funcs.my_record
--         FROM generate_series(1,3) val;

-- INSERT INTO o_test_custom_opclass
--     SELECT val, (val, (enum_range(NULL::my_funcs.my_enum))[
--                         ((val - 1) % 
--                          array_length(enum_range(NULL::my_funcs.my_enum), 1)) + 
--                          1
--                       ]
--                 )::my_funcs.my_record
--     FROM generate_series(1,10) val;
SELECT * FROM o_test_custom_opclass;
\q -- TODO: Remove

CREATE FUNCTION my_funcs.my_rec_cmp(a my_funcs.my_record, b my_funcs.my_record)
RETURNS integer
AS $$
	SELECT (array_length(enum_range(NULL, a.val_2), 1) -
			array_length(enum_range(NULL, b.val_2), 1));
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE FUNCTION my_funcs.my_rec_lt(a my_funcs.my_record, b my_funcs.my_record)
RETURNS boolean
AS $$
	BEGIN
	RETURN my_funcs.my_rec_cmp(a,b) < 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_funcs.my_rec_eq(a my_funcs.my_record, b my_funcs.my_record)
RETURNS boolean
AS $$
	BEGIN
	RETURN my_funcs.my_rec_cmp(a,b) = 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_funcs.my_rec_gt(a my_funcs.my_record, b my_funcs.my_record)
RETURNS boolean
AS $$
	BEGIN
	RETURN my_funcs.my_rec_cmp(a,b) > 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR my_funcs.#<# (
	LEFTARG = my_funcs.my_record,
	RIGHTARG = my_funcs.my_record,
	FUNCTION = my_funcs.my_rec_lt
);

CREATE OPERATOR my_funcs.#=# (
	LEFTARG = my_funcs.my_record,
	RIGHTARG = my_funcs.my_record,
	FUNCTION = my_funcs.my_rec_eq,
	MERGES
);

CREATE OPERATOR my_funcs.#># (
	LEFTARG = my_funcs.my_record,
	RIGHTARG = my_funcs.my_record,
	FUNCTION = my_funcs.my_rec_gt
);

CREATE OPERATOR CLASS my_funcs.capacity_ops
	DEFAULT FOR TYPE my_funcs.my_record
	USING btree AS
	OPERATOR 1 my_funcs.#<#,
	OPERATOR 3 my_funcs.#=#,
	OPERATOR 5 my_funcs.#>#,
	FUNCTION 1 my_funcs.my_rec_cmp(my_funcs.my_record, my_funcs.my_record);

CREATE INDEX rec_val_ix ON o_test_custom_opclass(rec_val);

set enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY int_val
	USING OPERATOR(my_funcs.<^);
SELECT * FROM o_test_custom_opclass ORDER BY int_val
	USING OPERATOR(my_funcs.<^);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val;
SELECT * FROM o_test_custom_opclass ORDER BY rec_val;
set enable_seqscan = on;

CREATE TYPE my_funcs.o_test_type_1 AS ENUM (
	'a', 'b', 'c', 'd', 'e', 'f');

CREATE TYPE my_funcs.o_test_type_2 AS (
	val_1 integer,
	val_2 my_funcs.o_test_type_1);

CREATE TABLE o_test_1 USING orioledb
AS SELECT ((random()*10), 
		   (ENUM_RANGE(
				NULL::my_funcs.o_test_type_1))[val])::my_funcs.o_test_type_2 
	AS val_1
	FROM generate_series(1,10) val;

CREATE FUNCTION my_funcs.func_test_1(a o_test_type_2) RETURNS numeric
AS $$
	SELECT a.val_1;
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE FUNCTION my_funcs.func_test_2(a o_test_type_2, b o_test_type_2)
RETURNS integer 
AS $$
	SELECT (my_funcs.func_test_1(a) - my_funcs.func_test_1(b));
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE FUNCTION my_funcs.func_test_3(a o_test_type_2, b o_test_type_2)
RETURNS boolean
AS $$
	BEGIN
	RETURN my_funcs.func_test_2(a,b) < 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR my_funcs.#<# (
	LEFTARG = my_funcs.o_test_type_2,
	RIGHTARG = my_funcs.o_test_type_2,
	FUNCTION = my_funcs.func_test_3);

CREATE OPERATOR CLASS my_funcs.test_class
	FOR TYPE my_funcs.o_test_type_2
	USING btree AS
	OPERATOR 1 my_funcs.#<#,
	FUNCTION 1 my_funcs.func_test_2(my_funcs.o_test_type_2,
									my_funcs.o_test_type_2);

CREATE INDEX val_ind ON o_test_1(val_1 my_funcs.test_class);

DROP SCHEMA my_funcs CASCADE;
DROP EXTENSION orioledb CASCADE;
