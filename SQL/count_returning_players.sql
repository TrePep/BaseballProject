SELECT 2000 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2000
UNION ALL
SELECT 2001 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2001
UNION ALL
SELECT 2002 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2002
UNION ALL
SELECT 2003 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2003
UNION ALL
SELECT 2004 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2004
UNION ALL
SELECT 2005 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2005
UNION ALL
SELECT 2006 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2006
UNION ALL
SELECT 2007 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2007
UNION ALL
SELECT 2008 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2008
UNION ALL
SELECT 2009 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2009
UNION ALL
SELECT 2010 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2010
UNION ALL
SELECT 2011 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2011
UNION ALL
SELECT 2012 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2012
UNION ALL
SELECT 2013 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2013
UNION ALL
SELECT 2014 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2014
UNION ALL
SELECT 2015 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2015
UNION ALL
SELECT 2016 as year, COUNT(*) as player_count FROM yearly_results.players_returning_after_gap_2016
ORDER BY year;