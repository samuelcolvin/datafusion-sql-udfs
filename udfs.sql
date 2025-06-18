CREATE FUNCTION add_one(a bigint) RETURNS bigint AS $$
    SELECT a + 1 FROM arguments;
$$ LANGUAGE SQL;

CREATE FUNCTION concat_strings(s1 text, s2 text) RETURNS text AS $$
    SELECT s1 || s2 FROM arguments;
$$ LANGUAGE SQL;
