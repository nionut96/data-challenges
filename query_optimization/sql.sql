

	-- Let's see what we have...

	select * from data_challenges.sp500;		--  500; instr_id
	select * from data_challenges.id_map;		-- 9876; id, instr_id, name
	select * from data_challenges.esg_scores;	-- 7125; id, total_score, e_score, s_score, g_score
	
	-- Noticed the following correlations (both from schema & data):
		-- sp500.instr_id == id_map.instr_id
		-- id_map.id == esg_scores.id
	
	
-- Determine the most efficient method of joining the included sp500, id_map, and esg_scores tables

	-- Let's create needed indexes

	create index on data_challenges.sp500 using hash (instr_id);
	create index on data_challenges.id_map using btree (id);
	create index on data_challenges.esg_scores using btree (id);

	-- By "the most efficient method of joining", I think it is about "inner join":

	select * 
	from data_challenges.sp500 dcsp -- using the smallest table (also the first ring in the chain)
	join data_challenges.id_map dcim on dcsp.instr_id = dcim.instr_id -- natural join
	join data_challenges.esg_scores dces on dcim.id = dces.id -- natural join
	; -- 292 rows

	-- Also, if by "joining", all the data is needed to be included, then let's use full join:

	select * 
	from data_challenges.sp500 dcsp
	full join data_challenges.id_map dcim on dcsp.instr_id = dcim.instr_id
	full join data_challenges.esg_scores dces on dcim.id = dces.id
	; -- 11127 rows


-- Create a new sp500_esg_scores table that lists all available identifier, descriptive, and score columns for the S&P 500 constituent entities

	-- Alright, this one clearly is a full join:
	
	-- drop table if exists data_challenges.sp500_esg_scores;
	create table data_challenges.sp500_esg_scores as
		select 
			dcsp.instr_id as "InstrumentID", dcim.id as "OtherID", -- identifiers
			dcim.name as "Descriptive", -- descriptive
			dces.total_score as "TotalScore", dces.e_score as "E_Score", dces.s_score as "S_Score", dces.g_score as "G_Score" -- score columns
		from data_challenges.sp500 dcsp
		full join data_challenges.id_map dcim on dcsp.instr_id = dcim.instr_id
		full join data_challenges.esg_scores dces on dcim.id = dces.id
	;
	select * from data_challenges.sp500_esg_scores; -- 11127 rows
	
-- Add a rank column to the new sp500_esg_scores table that ranks the S&P 500 constituent entities by percentile on total_score in ascending order

	-- LATER EDIT: You may skip below trials & errors to "AFTER TONY's RESPONSE TO EMAIL", if you want :)

	-- hmm.. what is a percentile? googling time!
	-- it looks like "percentile" is not the same thing as "percentage" -> new opportunity of learning
	-- this sounds pretty similar to "rank" window function, but with percentages
	-- okay, now I think I got it
	
	-- How should be handled the rows with null TotalScore? I think I will consider them as TotalScore = 0

	select 
		*,
		coalesce("TotalScore", 0) "TotalScoreCoalesced", 
		(
			(count(*) over () - rank() over (order by "TotalScore" desc nulls last))::numeric(15, 6) 
			* 100 / count(*) over ()
		)::numeric(15, 2)
	from data_challenges.sp500_esg_scores
	order by "TotalScoreCoalesced"
	;
	
	-- looks like it doesn't do what we need... there should be at least 1 value of "0"... go back to research
	
	-- oh, wait, there is "percent_rank"...
	
	select 
		--*, 
		coalesce("TotalScore", 0) "TotalScoreCoalesced", 
		((percent_rank() over (order by "TotalScore" asc nulls first)) * 100)::numeric(15, 2)
	from data_challenges.sp500_esg_scores
	order by "TotalScoreCoalesced" desc
	;
	
	-- looks like it doesn't do what we need... there shouldn't be a "100" value... go back to research 
	-- (LATER EDIT: "100" was okay, but this does not cover a particular case - explained below)
	
	-- let's try to do them manually... https://www.youtube.com/watch?v=Snf6Wpn-L4c
	
	select *
	from data_challenges.sp500_esg_scores dcspes
	cross join lateral (
		select (
			(select count("TotalScore") from data_challenges.sp500_esg_scores t where coalesce(t."TotalScore", 0) < coalesce(dcspes."TotalScore", 0))::numeric
			/
			(select count("TotalScore") from data_challenges.sp500_esg_scores)
			*
			100
		)::numeric(15, 6) as "Percentile"
	) tmp
	order by "Percentile"
	;
	
	-- alright.. I think I like the resulting table, but.. it runs AWFULLY SLOW.. we don't like this
	-- (LATER EDIT: nulls and zeros are ranked equally -> we don't want this)
	
	-- AFTER TONY's RESPONSE TO EMAIL:
	
	-- Let's take the "percent_rank" again:
	
	select *,
		(percent_rank() over (order by "TotalScore" asc nulls first)) percentile
	from data_challenges.sp500_esg_scores dcspes
	order by percentile
	;
	
	-- I don't really like this method, because:
	-- if we have only one "maximum" value, this value is calculated as "1.0" (100th percentile),
	-- but if we have MULTIPLE "maximum" values, those values are NOT calculated as "1.0" (100th percentile), but something like 0.9999...
	-- insert into data_challenges.sp500_esg_scores ("TotalScore") values (100); -- delete from data_challenges.sp500_esg_scores where "TotalScore" = 100.0 and "Descriptive" is null and "E_Score" is null;
	-- From e-mail -> "lowest values (here, these will be your NULLs) are at 0 and your highest values are at 1"
	-- let's try to make our own calculations, using rank & max:
	
	select *,
		(
			(max(tmp.rnk) over () - tmp.rnk) * 100 / max(tmp.rnk) over ()
		) -- ::numeric(15, 2) -- leaving to it's default precision
		as percentile
	from (
		select *,
			-- the (-1) is to obtain later the percentile 0 for the lowest values
			(rank() over (order by "TotalScore" desc nulls last) - 1)::numeric as rnk -- nulls will be percentile-ranked as "lower" than score=0
			-- (rank() over (order by coalesce("TotalScore", 0) desc nulls last) - 1)::numeric as rnk -- nulls will be percentile-ranked as "equal" to score=0
		from data_challenges.sp500_esg_scores dcspes
	) tmp
	order by "TotalScore" nulls first
	;
	
	-- To be further discussed if the result is not the expected one.
	
	-- Until then, let's go forward with this version, updating the table:
	
	alter table data_challenges.sp500_esg_scores add column "Rank" numeric(15, 6);
	
	drop sequence if exists id_seq;
	create temp sequence id_seq start with -1 increment by -1;
	update data_challenges.sp500_esg_scores set "OtherID" = nextval('id_seq') where "OtherID" is null;

	drop table if exists tmp;
	create temp table tmp as
		select
			*, 
			(max(t.rnk) over () - t.rnk) * 100 / max(t.rnk) over () as percentile
		from (
			select *, (rank() over (order by "TotalScore" desc nulls last) - 1)::numeric as rnk
			from data_challenges.sp500_esg_scores dcspes
		) t
	;
	create index on tmp using btree ("OtherID");
	select * from tmp order by "OtherID";
	select * from tmp order by "Rank";

	select count(*), count("OtherID") from data_challenges.sp500_esg_scores; -- 11127 = 11127 -> unique values

	update data_challenges.sp500_esg_scores dcspes
	set "Rank" = (
		select percentile
		from tmp
		where tmp."OtherID" = dcspes."OtherID"
	)
	;
	
	update data_challenges.sp500_esg_scores set "OtherID" = null where "OtherID" < 0;

	vacuum full analyze data_challenges.sp500_esg_scores;
	
	select * from data_challenges.sp500_esg_scores order by "Rank" desc;
	
	-- now we can have percentile 100 for all values that are "maximum"
	

-- Add a TOTAL row to the sp500_esg_scores table that shows the median value for each score column across the S&P 500 constituents

	-- delete from data_challenges.sp500_esg_scores dcspes where "Descriptive" = 'TOTAL';
	
	insert into data_challenges.sp500_esg_scores
		select 
			'MEDIAN - DISCRETE', null, 'TOTAL', 
			
			percentile_disc(0.5) within group (order by coalesce("TotalScore", 0) asc),
			percentile_disc(0.5) within group (order by coalesce("E_Score", 0) asc),
			percentile_disc(0.5) within group (order by coalesce("S_Score", 0) asc),
			percentile_disc(0.5) within group (order by coalesce("G_Score", 0) asc),
			
			-1
			
			from data_challenges.sp500_esg_scores
	;

	insert into data_challenges.sp500_esg_scores
		select 
			'MEDIAN - INTERPOLATED', null, 'TOTAL', 
			
			percentile_cont(0.5) within group (order by coalesce("TotalScore", 0) asc),
			percentile_cont(0.5) within group (order by coalesce("E_Score", 0) asc),
			percentile_cont(0.5) within group (order by coalesce("S_Score", 0) asc),
			percentile_cont(0.5) within group (order by coalesce("G_Score", 0) asc),
			
			-1
			
			from data_challenges.sp500_esg_scores
	;

	-- Which one of those two medians should remain? Because they may vary.
	-- I will leave the both of them.
	
	-- curiosity: Why is it called 'TOTAL' if it is about median values?
	-- note: I don't always use uppercase column names
	

-- Make sure that where S&P 500 constituent entities are missing score values they still appear in the sp500_esg_scores table and are ranked in the 0 percentile

	select * from data_challenges.sp500_esg_scores where "TotalScore" is null and "Rank" != 0;
	select * from data_challenges.sp500_esg_scores where "TotalScore" is null and "Rank" = 0;

	select * from data_challenges.sp500_esg_scores where "Descriptive" = 'TOTAL';

-- Other thoughts:
/*
	Maybe some VIEWS would work better than TABLES, in order to manipulate data.
	
	As a better design, the median values should not lay in that table, 
		but for the sake of the challenge, I think it's perfectly okay.
	
	Overall, nice challenge! Gimme more! :)
*/
