CREATE FUNCTION add_one(a bigint) RETURNS bigint AS $$
    SELECT a + 1 FROM arguments;
$$ LANGUAGE SQL;

CREATE FUNCTION concat_strings(s1 text, s2 text) RETURNS text AS $$
    SELECT s1 || s2 FROM arguments;
$$ LANGUAGE SQL;


CREATE FUNCTION llm_cost(model text, input_tokens double, output_tokens double) RETURNS double AS $$
    SELECT CASE
        WHEN model = 'gpt-4o' THEN
          2.50 * input_tokens + output_tokens * 10.00
        WHEN model = 'gpt-4o-mini' THEN
          0.150 * input_tokens + output_tokens * 0.60
        ELSE null
        END
    from arguments;
$$ LANGUAGE SQL;
