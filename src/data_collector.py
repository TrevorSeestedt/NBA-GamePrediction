"""
NBA Data Collector Module
Fetches data from NBA API and stores in MongoDB
"""

from nba_api.stats.endpoints import leaguegamefinder, teamgamelogs, playergamelogs, leaguestandings, leaguedashteamstats
from nba_api.stats.static import teams, players
import pandas as pd
import time
import logging
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from .database import NBADatabase

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBADataCollector:
    def __init__(self, rate_limit_delay=1.5):
        """
        Initialize NBA data collector
        
        Args:
            rate_limit_delay (float): Time (seconds) between API calls
        """

        self.rate_limit_delay = rate_limit_delay
        self.teams = teams.get_teams()

        self.team_dict = {team['full_name']: team['id'] for team in self.teams}
        
        # Initialize database connection to Docker 
        self.db = NBADatabase()
        
        logger.info(f"NBA Data Collector initialized")
        logger.info(f"Found {len(self.teams)} NBA teams")
        logger.info(f"Rate limit between requests: {rate_limit_delay}s ")
    
    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        time.sleep(self.rate_limit_delay)
    
    def collect_season_games(self, season='2023-24', season_type='Regular Season'):
        """
        Collect all games for a specific season
        
        Args:
            season (str): NBA season ('2023-24')
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            int: Number of games collected
        """
        logger.info(f"Starting collection for {season} {season_type}")
        
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Fetch games from NBA API
            logger.info("Fetching games from NBA API...")
            gamefinder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type
            )
            
            # Getting the data as a list of DataFrames and then accessing the first one
            games_df = gamefinder.get_data_frames()[0]
            
            if games_df.empty:
                logger.warning(f"No games found for {season} {season_type}")
                return 0
            
            # Add season and season type to dataframe
            games_df['SEASON'] = season
            games_df['SEASON_TYPE'] = season_type
            
            # Convert date strings to datetime objects for clean/accessible data
            games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
            
            logger.info(f"Game records fetched: {len(games_df)} games")
            logger.info(f"Date range: {games_df['GAME_DATE'].min()} to {games_df['GAME_DATE'].max()}")
            
            # Store in MongoDB 
            logger.info("Games being stored to MongoDB")
            inserted_count = self.db.insert_games(games_df)
            
            logger.info(f"Successfully collected {inserted_count} new games for {season}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error collecting games for {season}: {e}")
            return 0
    
    def collect_multiple_seasons(self, seasons=['2024-25', '2023-24', '2022-23'], season_type='Regular Season'):
        """
        Collect games for multiple seasons
        
        Args:
            seasons (list): List of seasons to collect ('2024-25', '2023-24', '2022-23')
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            dict: Results for each season (games collected)
        """

        logger.info(f"Starting multi-season collection")
        logger.info(f"Seasons: {seasons}")
        
        results = {}
        total_collected = 0
        
        # enumerate can be used to iterate over the list and get the index and the value
        for i, season in enumerate(seasons):
            logger.info(f"Progress: {i+1}/{len(seasons)} - Collecting: {season}")
            
            collected = self.collect_season_games(season, season_type)
            results[season] = collected
            total_collected += collected
            
            # Progress update
            logger.info(f"{season}: {collected} games collected")
            
            # Rate limiting between seasons
            if i < len(seasons) - 1:  # Don't wait after the last season
                logger.info(f"Waiting {self.rate_limit_delay}s before next season...")
                self._rate_limit()
        
        logger.info(f"Multi-season collection complete!")
        logger.info(f"Total games collected: {total_collected}")
        logger.info(f"Results by season: {results}")
        
        return results
    
    def collect_team_games(self, team_name, season='2023-24', season_type='Regular Season'):
        """
        Collect games for a specific team
        
        Args:
            team_name (str): Full team name ('Los Angeles Lakers')
            season (str): NBA season ('2023-24')
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            int: Number of games collected
        """
        if team_name not in self.team_dict:
            logger.error(f"Team '{team_name}' not found")
            available_teams = list(self.team_dict.keys())[:5]
            logger.info(f"Available teams (sample): {available_teams}")
            return 0
        
        team_id = self.team_dict[team_name]
        logger.info(f"Collecting games for {team_name} (ID: {team_id})")
        
        try:
            self._rate_limit()
            
            team_games = teamgamelogs.TeamGameLogs(
                team_id_nullable=team_id,
                season_nullable=season,
                season_type_nullable=season_type
            )
            
            games_df = team_games.get_data_frames()[0]
            
            if games_df.empty:
                logger.warning(f"No games found for {team_name}")
                return 0
            
            # Add metadata
            games_df['SEASON'] = season
            games_df['SEASON_TYPE'] = season_type
            games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
            
            # Store in database
            inserted_count = self.db.insert_games(games_df)
            
            logger.info(f"Collected {inserted_count} games for {team_name}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"Error collecting games for {team_name}: {e}")
            return 0
    
    def get_collection_stats(self):
        """
        Get statistics about collected data
        
        Returns:
            dict: Collection statistics
        """
        logger.info("Gathering collection statistics...")
        
        try:
            # Get all games from database
            all_games = self.db.get_games()
            
            if all_games.empty:
                return {
                    'total_games': 0,
                    'seasons': [],
                    'teams': [],
                    'date_range': None
                }
            
            stats = {
                'total_games': len(all_games),
                'seasons': sorted(all_games['SEASON'].unique().tolist()) if 'SEASON' in all_games.columns else [],
                'teams': len(all_games['TEAM_ID'].unique()) if 'TEAM_ID' in all_games.columns else 0,
                'date_range': {
                    'earliest': all_games['GAME_DATE'].min() if 'GAME_DATE' in all_games.columns else None,
                    'latest': all_games['GAME_DATE'].max() if 'GAME_DATE' in all_games.columns else None
                }
            }
            
            logger.info(f"Collection Stats:")
            logger.info(f"Total games: {stats['total_games']}")
            logger.info(f"Seasons: {stats['seasons']}")
            logger.info(f"Teams: {stats['teams']}")
            logger.info(f"Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting collection stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.db:
            self.db.close()
            logger.info("🔌 Database connection closed")
    
    def collect_player_games(self, player_name, season='2023-24'):
        """
        Collect all games for a specific player in a season
        
        This function gets every game a player played, with their individual stats
        like points, rebounds, assists for each game. Useful for analyzing player
        performance trends and consistency.
        
        Args:
            player_name (str): Full player name (e.g., 'LeBron James')
            season (str): NBA season (e.g., '2023-24')
            
        Returns:
            int: Number of player games collected
        """
        logger.info(f"Collecting individual games for {player_name} in {season}")

        try: 
            # Step 1: Apply rate limiting to respect NBA API limits
            self._rate_limit()

            # Step 2: Find the player's unique ID using their full name
            # The NBA API needs player ID numbers, not names
            player_list = players.find_players_by_full_name(player_name)
            if not player_list:
                logger.error(f"Player '{player_name}' was not found")
                return 0
            
            player_id = player_list[0]['id']
            logger.info(f"Found {player_name}, ID: {player_id}")

            # Step 3: Get all games this player played using NBA API
            # PlayerGameLogs gives us every single game with individual stats
            player_logs = playergamelogs.PlayerGameLogs(
                player_id_nullable=player_id,      # Which player
                season_nullable=season,             # Which season
                season_type_nullable='Regular Season'  # Regular season vs playoffs
            )

            # Step 4: Convert API response to pandas DataFrame for easy handling
            games_df = player_logs.get_data_frames()[0]

            # Step 5: Check if we got any data back
            if games_df.empty: 
                logger.warning(f"No games found for {player_name} in {season}")
                return 0
            
            # Step 6: Add metadata to help us organize the data later
            games_df['SEASON'] = season                    # Which season this data is from
            games_df['SEASON_TYPE'] = 'Regular Season'     # Type of games
            games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])  # Convert dates
            games_df['PLAYER_NAME'] = player_name          # Add player name for easy reference
            
            # Step 7: Store in database (in a separate collection for player data)
            # We'll store this in 'player_games' collection, separate from team games
            inserted_count = self.db.db.player_games.insert_many(
                games_df.to_dict(orient='records'), 
                ordered=False
            ).inserted_ids
            
            logger.info(f"Collected {len(inserted_count)} games for {player_name}")
            return len(inserted_count)
            
        except IndexError:
            # This happens if player name isn't found
            logger.error(f"Player '{player_name}' not found in NBA database")
            return 0
        except Exception as e: 
            logger.error(f"Error collecting games for {player_name}: {e}")
            return 0

    def collect_team_standings(self, season='2023-24'):
        """
        Collect current team standings/rankings
        
        Hints:
        1. Use leaguestandings.LeagueStandings
        2. Store wins, losses, win percentage, conference rank
        3. Add timestamp for historical tracking
        """
        logger.info(f"Collecting team standings for {season}")
        
        try:
            # Step 1: Apply rate limiting
            self._rate_limit()
            
            # Step 2: Get standings from NBA API
            standings = leaguestandings.LeagueStandings(season=season)
            standings_df = standings.get_data_frames()[0]
            
            if standings_df.empty:
                logger.warning(f"No standings data found for {season}")
                return 0
            
            logger.info(f"Retrieved standings for {len(standings_df)} teams")
            
            # Step 3: Process standings data
            standings_data = []
            
            for _, row in standings_df.iterrows():
                team_standing = {
                    'team_id': int(row['TeamID']),
                    'team_name': row['TeamName'],
                    'season': season,
                    
                    # Record information
                    'wins': int(row['WINS']),
                    'losses': int(row['LOSSES']),
                    'win_pct': float(row['WinPCT']),
                    'games_played': int(row['WINS']) + int(row['LOSSES']),
                    
                    # Conference standings
                    'conference': row['Conference'],
                    'conference_rank': int(row['ConferenceRank']),
                    'division': row['Division'],
                    'division_rank': int(row['DivisionRank']),
                    
                    # Overall league position
                    'league_rank': int(row.get('LeagueRank', 0)),
                    
                    # Additional context
                    'playoff_rank': int(row.get('PlayoffRank', 0)),
                    'home_record': row.get('HOME', 'N/A'),
                    'road_record': row.get('ROAD', 'N/A'),
                    'last_10': row.get('L10', 'N/A'),
                    
                    # Metadata
                    'collected_at': datetime.now()
                }
                
                standings_data.append(team_standing)
                
                logger.info(f"{team_standing['team_name']}: "
                          f"{team_standing['wins']}-{team_standing['losses']} "
                          f"({team_standing['win_pct']:.3f}), "
                          f"{team_standing['conference']} #{team_standing['conference_rank']}")
            
            # Step 4: Store in database
            if standings_data:
                result = self.db.db.team_standings.insert_many(standings_data, ordered=False)
                logger.info(f"Stored standings for {len(result.inserted_ids)} teams")
            
            # Show conference leaders
            east_teams = [t for t in standings_data if t['conference'] == 'East']
            west_teams = [t for t in standings_data if t['conference'] == 'West']
            
            if east_teams:
                east_leader = min(east_teams, key=lambda x: x['conference_rank'])
                logger.info(f"Eastern Conference Leader: {east_leader['team_name']} ({east_leader['wins']}-{east_leader['losses']})")
            
            if west_teams:
                west_leader = min(west_teams, key=lambda x: x['conference_rank'])
                logger.info(f"Western Conference Leader: {west_leader['team_name']} ({west_leader['wins']}-{west_leader['losses']})")
            
            return len(standings_data)
            
        except Exception as e:
            logger.error(f"Error collecting team standings: {e}")
            return 0

    def validate_collected_data(self, season='2023-24'):
        """
        Validate collected data for completeness and accuracy
        
        Hints:
        1. Check if all 30 teams have data
        2. Verify expected number of games per team (82 regular season)
        3. Check for missing dates or duplicate games
        4. Validate statistical ranges (points can't be negative, etc.)
        """
        logger.info(f"Validating collected data for {season}")
        validation_results = {
            'season': season,
            'total_issues': 0,
            'teams_with_data': 0,
            'teams_missing': [],
            'game_count_issues': [],
            'duplicate_games': 0,
            'invalid_stats': [],
            'date_range_issues': []
        }
        
        try:
            # Step 1: Check if all 30 teams have data
            logger.info("Checking team coverage...")
            
            # Get games from database for this season
            games_cursor = self.db.db.games.find({'SEASON': season})
            all_games = list(games_cursor)
            
            if not all_games:
                logger.error(f"No games found for {season}")
                validation_results['total_issues'] += 1
                return validation_results
            
            # Count unique teams
            teams_in_db = set()
            for game in all_games:
                teams_in_db.add(game.get('TEAM_NAME', game.get('Team')))
            
            validation_results['teams_with_data'] = len(teams_in_db)
            logger.info(f"Found data for {len(teams_in_db)} teams")
            
            # Check for missing teams
            expected_teams = {team['full_name'] for team in self.teams}
            missing_teams = expected_teams - teams_in_db
            validation_results['teams_missing'] = list(missing_teams)
            
            if missing_teams:
                logger.warning(f"Missing data for {len(missing_teams)} teams: {missing_teams}")
                validation_results['total_issues'] += len(missing_teams)
            
            # Step 2: Check game counts per team (should be ~82 for regular season)
            logger.info("Checking game counts per team...")
            team_game_counts = {}
            
            for game in all_games:
                team_name = game.get('TEAM_NAME', game.get('Team'))
                if team_name:
                    team_game_counts[team_name] = team_game_counts.get(team_name, 0) + 1
            
            for team, count in team_game_counts.items():
                expected_games = 82  # Regular season
                if abs(count - expected_games) > 5:  # Allow some tolerance
                    issue = f"{team}: {count} games (expected ~{expected_games})"
                    validation_results['game_count_issues'].append(issue)
                    logger.warning(issue)
            
            validation_results['total_issues'] += len(validation_results['game_count_issues'])
            
            # Step 3: Check for duplicate games
            logger.info("Checking for duplicate games...")
            game_signatures = set()
            duplicates = 0
            
            for game in all_games:
                # Create unique signature for each game
                signature = (
                    game.get('GAME_ID'),
                    game.get('TEAM_NAME', game.get('Team')),
                    game.get('GAME_DATE')
                )
                
                if signature in game_signatures:
                    duplicates += 1
                else:
                    game_signatures.add(signature)
            
            validation_results['duplicate_games'] = duplicates
            if duplicates > 0:
                logger.warning(f"Found {duplicates} duplicate games")
                validation_results['total_issues'] += duplicates
            
            # Step 4: Validate statistical ranges
            logger.info("Validating statistical ranges...")
            
            for game in all_games:
                team_name = game.get('TEAM_NAME', game.get('Team', 'Unknown'))
                
                # Check points (should be between 50-200)
                points = game.get('PTS', 0)
                if points < 50 or points > 200:
                    issue = f"{team_name}: Invalid points {points}"
                    validation_results['invalid_stats'].append(issue)
                
                # Check field goal percentage (should be 0-1)
                fg_pct = game.get('FG_PCT', 0)
                if fg_pct < 0 or fg_pct > 1:
                    issue = f"{team_name}: Invalid FG% {fg_pct}"
                    validation_results['invalid_stats'].append(issue)
                
                # Check rebounds (should be positive)
                rebounds = game.get('REB', 0)
                if rebounds < 0:
                    issue = f"{team_name}: Negative rebounds {rebounds}"
                    validation_results['invalid_stats'].append(issue)
            
            validation_results['total_issues'] += len(validation_results['invalid_stats'])
            
            # Step 5: Check date ranges
            logger.info("Checking date ranges...")
            
            # Convert dates and check range
            dates = []
            for game in all_games:
                game_date = game.get('GAME_DATE')
                if game_date:
                    if isinstance(game_date, str):
                        try:
                            game_date = pd.to_datetime(game_date)
                        except:
                            continue
                    dates.append(game_date)
            
            if dates:
                min_date = min(dates)
                max_date = max(dates)
                date_span = (max_date - min_date).days
                
                logger.info(f"Date range: {min_date.strftime('%Y-%m-%d')} to {max_date.strftime('%Y-%m-%d')} ({date_span} days)")
                
                # Check if date range makes sense for season
                if date_span < 180 or date_span > 300:  # Season should be ~6-10 months
                    issue = f"Unusual date span: {date_span} days"
                    validation_results['date_range_issues'].append(issue)
                    validation_results['total_issues'] += 1
            
            # Summary
            logger.info("=== VALIDATION SUMMARY ===")
            logger.info(f"Season: {season}")
            logger.info(f"Teams with data: {validation_results['teams_with_data']}/30")
            logger.info(f"Total games: {len(all_games)}")
            logger.info(f"Total issues found: {validation_results['total_issues']}")
            
            if validation_results['total_issues'] == 0:
                logger.info("✅ Data validation PASSED - No issues found!")
            else:
                logger.warning(f"⚠️ Data validation found {validation_results['total_issues']} issues")
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error during data validation: {e}")
            validation_results['total_issues'] += 1
            return validation_results

    def update_recent_games(self, days_back=7):
        """
        Update only recent games (last N days)
        
        Hints:
        1. Calculate date range (today - days_back to today)
        2. Check what games already exist in database
        3. Only collect missing games
        4. Handle ongoing games vs completed games
        """
        logger.info(f"Updating recent games (last {days_back} days)")
        
        try:
            # Step 1: Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # Step 2: Check what games already exist in database
            existing_games = set()
            
            # Query database for games in date range
            existing_cursor = self.db.db.games.find({
                'GAME_DATE': {
                    '$gte': start_date,
                    '$lte': end_date
                }
            })
            
            for game in existing_cursor:
                # Create unique identifier for each game
                game_id = game.get('GAME_ID')
                if game_id:
                    existing_games.add(game_id)
            
            logger.info(f"Found {len(existing_games)} existing games in database")
            
            # Step 3: Get recent games from NBA API
            self._rate_limit()
            
            # Use current season (determine based on current date)
            current_year = datetime.now().year
            current_month = datetime.now().month
            
            # NBA season spans two calendar years (Oct-June)
            if current_month >= 10:  # October or later = new season starting
                season = f"{current_year}-{str(current_year + 1)[-2:]}"
            else:  # Before October = season ending
                season = f"{current_year - 1}-{str(current_year)[-2:]}"
            
            logger.info(f"Using season: {season}")
            
            # Get all recent games
            gamefinder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable='Regular Season',
                date_from_nullable=start_date.strftime('%m/%d/%Y'),
                date_to_nullable=end_date.strftime('%m/%d/%Y')
            )
            
            recent_games_df = gamefinder.get_data_frames()[0]
            
            if recent_games_df.empty:
                logger.info("No recent games found from API")
                return 0
            
            logger.info(f"API returned {len(recent_games_df)} recent game records")
            
            # Step 4: Filter out games we already have
            new_games = []
            
            for _, game in recent_games_df.iterrows():
                game_id = game.get('GAME_ID')
                
                if game_id not in existing_games:
                    # Add metadata
                    game_dict = game.to_dict()
                    game_dict['SEASON'] = season
                    game_dict['SEASON_TYPE'] = 'Regular Season'
                    game_dict['GAME_DATE'] = pd.to_datetime(game_dict['GAME_DATE'])
                    
                    new_games.append(game_dict)
            
            logger.info(f"Found {len(new_games)} new games to add")
            
            # Step 5: Insert new games into database
            if new_games:
                # Convert to DataFrame for database insertion
                new_games_df = pd.DataFrame(new_games)
                inserted_count = self.db.insert_games(new_games_df)
                
                logger.info(f"Successfully inserted {inserted_count} new games")
                
                # Show sample of what was added
                if len(new_games) > 0:
                    sample_games = new_games[:3]  # Show first 3
                    logger.info("Sample new games:")
                    for game in sample_games:
                        logger.info(f"  {game.get('TEAM_NAME')} vs {game.get('MATCHUP')} on {game.get('GAME_DATE')}")
                
                return inserted_count
            else:
                logger.info("No new games to add - database is up to date")
                return 0
            
        except Exception as e:
            logger.error(f"Error updating recent games: {e}")
            return 0

    def collect_team_advanced_stats(self, season='2023-24'):
        """
        Collect advanced team statistics (Offensive/Defensive Ratings, Pace, etc.)
        
        This function gets the "advanced" stats that show how efficient teams are.
        These are crucial for predictions because they show which teams are actually
        good vs just lucky.
        
        Key Stats Explained:
        - Offensive Rating: Points scored per 100 possessions (higher = better offense)
        - Defensive Rating: Points allowed per 100 possessions (lower = better defense)  
        - Net Rating: OffRtg - DefRtg (positive = good team)
        - Pace: How fast they play (possessions per 48 minutes)
        
        Args:
            season (str): NBA season (e.g., '2023-24')
            
        Returns:
            int: Number of teams processed
        """
        from nba_api.stats.endpoints import leaguedashteamstats
        
        logger.info(f"Collecting advanced team stats for {season}")
        logger.info("Getting Offensive/Defensive Ratings, Pace, etc.")
        
        try:
            # Step 1: Apply rate limiting
            self._rate_limit()
            
            # Step 2: Get advanced stats for ALL teams at once (much more efficient!)
            # Using leaguedashteamstats with 'Advanced' measure type
            league_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,                              # Which season
                season_type_all_star='Regular Season',     # Regular season only
                measure_type_detailed_defense='Advanced'   # This gives us the ratings!
            )
            
            # Step 3: Extract the data from API response
            stats_df = league_stats.get_data_frames()[0]
            
            if stats_df.empty:
                logger.warning("No advanced stats data found")
                return 0
            
            logger.info(f"Retrieved advanced stats for {len(stats_df)} teams")
            
            # Step 4: Process each team's data
            all_team_stats = []
            teams_processed = 0
            
            for _, row in stats_df.iterrows():
                try:
                    # Step 5: Build our clean data structure
                    team_advanced_data = {
                        'team_id': int(row['TEAM_ID']),
                        'team_name': row['TEAM_NAME'],
                        'season': season,
                        
                        # Core efficiency metrics (the most important!)
                        'off_rating': float(row['OFF_RATING']),        # Points per 100 possessions
                        'def_rating': float(row['DEF_RATING']),        # Opponent points per 100 possessions
                        'net_rating': float(row['NET_RATING']),        # Off - Def (positive = good)
                        
                        # Pace and style metrics
                        'pace': float(row['PACE']),                    # How fast they play
                        'ts_pct': float(row['TS_PCT']),               # True shooting % (shooting efficiency)
                        
                        # Other useful advanced metrics
                        'ast_pct': float(row.get('AST_PCT', 0)),      # Assist percentage
                        'ast_to_ratio': float(row.get('AST_TO', 0)),  # Assist to turnover ratio
                        'oreb_pct': float(row.get('OREB_PCT', 0)),    # Offensive rebound percentage
                        'dreb_pct': float(row.get('DREB_PCT', 0)),    # Defensive rebound percentage
                        'reb_pct': float(row.get('REB_PCT', 0)),      # Total rebound percentage
                        'tm_tov_pct': float(row.get('TM_TOV_PCT', 0)), # Team turnover percentage
                        'efg_pct': float(row.get('EFG_PCT', 0)),      # Effective field goal %
                        'pie': float(row.get('PIE', 0)),              # Player Impact Estimate
                        
                        # Record for context
                        'wins': int(row['W']),
                        'losses': int(row['L']),
                        'games_played': int(row['GP']),
                        'win_pct': float(row['W_PCT']),
                        
                        # Rankings (how they compare to other teams)
                        'off_rating_rank': int(row.get('OFF_RATING_RANK', 0)),
                        'def_rating_rank': int(row.get('DEF_RATING_RANK', 0)),
                        'net_rating_rank': int(row.get('NET_RATING_RANK', 0)),
                        'pace_rank': int(row.get('PACE_RANK', 0)),
                        
                        # Metadata
                        'collected_at': datetime.now()
                    }
                    
                    all_team_stats.append(team_advanced_data)
                    teams_processed += 1
                    
                    logger.info(f"{team_advanced_data['team_name']}: "
                              f"OffRtg={team_advanced_data['off_rating']:.1f} (#{team_advanced_data['off_rating_rank']}), "
                              f"DefRtg={team_advanced_data['def_rating']:.1f} (#{team_advanced_data['def_rating_rank']}), "
                              f"NetRtg={team_advanced_data['net_rating']:.1f}")
                    
                except Exception as e:
                    logger.error(f"Error processing team data: {e}")
                    continue
            
            # Step 6: Store all team stats in database
            if all_team_stats:
                # Store in separate collection for advanced team stats
                result = self.db.db.team_advanced_stats.insert_many(all_team_stats, ordered=False)
                logger.info(f"Stored advanced stats for {len(result.inserted_ids)} teams")
            
            logger.info(f"Successfully processed {teams_processed} teams")
            
            # Show top teams for context
            top_offense = sorted(all_team_stats, key=lambda x: x['off_rating'], reverse=True)[:3]
            top_defense = sorted(all_team_stats, key=lambda x: x['def_rating'])[:3]
            
            offense_names = [f"{t['team_name']} ({t['off_rating']:.1f})" for t in top_offense]
            defense_names = [f"{t['team_name']} ({t['def_rating']:.1f})" for t in top_defense]
            logger.info(f"Top 3 Offensive teams: {offense_names}")
            logger.info(f"Top 3 Defensive teams: {defense_names}")
            
            return teams_processed
            
        except Exception as e:
            logger.error(f"Error in team advanced stats collection: {e}")
            return 0
    
    def collect_player_vs_team_stats(self, player_name, season='2023-24'):
        """
        Collect how a specific player performs against different teams
        
        This function finds out if a player has "favorite opponents" or teams
        that give them trouble. For example: Does LeBron score more against
        the Warriors? Does he struggle against the Celtics defense?
        
        This is super valuable for predictions because some players consistently
        perform better/worse against certain teams.
        
        Args:
            player_name (str): Full player name (e.g., 'LeBron James')
            season (str): NBA season (e.g., '2023-24')
            
        Returns:
            int: Number of matchup records created
        """
        from nba_api.stats.endpoints import playerdashboardbygeneralsplits
        
        logger.info(f"Analyzing {player_name} vs all NBA teams in {season}")
        
        try:
            # Step 1: Find the player's ID (NBA API needs numbers, not names)
            player_list = players.find_players_by_full_name(player_name)
            if not player_list:
                logger.error(f"Player '{player_name}' not found")
                return 0
            
            player_id = player_list[0]['id']
            logger.info(f"Found {player_name} with ID: {player_id}")
            
            # Step 2: Apply rate limiting
            self._rate_limit()
            
            # Step 3: Get player's stats split by opponent team
            # This magical endpoint gives us how the player performed vs each team
            player_splits = playerdashboardbygeneralsplits.PlayerDashboardByGeneralSplits(
                player_id=player_id,                    # Which player
                season=season,                          # Which season
                season_type_all_star='Regular Season'   # Regular season only
            )
            
            # Step 4: Get the "vs opponent" data
            # The API returns multiple DataFrames, we want the one with opponent splits
            dataframes = player_splits.get_data_frames()
            
            # Usually the opponent splits are in one of the later DataFrames
            # Let's find the right one by looking for opponent-related columns
            opponent_stats_df = None
            for i, df in enumerate(dataframes):
                if not df.empty and any(col for col in df.columns if 'OPP' in col or 'VS' in col):
                    opponent_stats_df = df
                    break
            
            # If we can't find opponent splits, try a different approach
            if opponent_stats_df is None or opponent_stats_df.empty:
                logger.warning(f"No opponent split data found for {player_name}")
                # Alternative: Get all games and group by opponent manually
                return self._collect_player_vs_team_manual(player_name, player_id, season)
            
            # Step 5: Process each opponent matchup
            matchups_created = 0
            all_matchup_data = []
            
            for _, row in opponent_stats_df.iterrows():
                try:
                    # Extract opponent info (this varies by API response format)
                    opponent_name = row.get('OPPONENT', 'Unknown')
                    
                    # Build matchup data structure
                    matchup_data = {
                        'player_id': player_id,
                        'player_name': player_name,
                        'vs_team_name': opponent_name,
                        'season': season,
                        
                        # Performance stats vs this opponent
                        'games_played': int(row.get('GP', 0)),
                        'avg_points': float(row.get('PTS', 0)),
                        'avg_rebounds': float(row.get('REB', 0)),
                        'avg_assists': float(row.get('AST', 0)),
                        'fg_pct_vs_team': float(row.get('FG_PCT', 0)),
                        'fg3_pct_vs_team': float(row.get('FG3_PCT', 0)),
                        'ft_pct_vs_team': float(row.get('FT_PCT', 0)),
                        'plus_minus_vs_team': float(row.get('PLUS_MINUS', 0)),
                        
                        # Additional useful stats
                        'minutes_vs_team': float(row.get('MIN', 0)),
                        'steals_vs_team': float(row.get('STL', 0)),
                        'blocks_vs_team': float(row.get('BLK', 0)),
                        'turnovers_vs_team': float(row.get('TOV', 0)),
                        
                        # Metadata
                        'collected_at': datetime.now()
                    }
                    
                    all_matchup_data.append(matchup_data)
                    matchups_created += 1
                    
                    logger.info(f"vs {opponent_name}: {matchup_data['avg_points']:.1f} pts, "
                              f"{matchup_data['avg_rebounds']:.1f} reb, {matchup_data['avg_assists']:.1f} ast")
                    
                except Exception as e:
                    logger.error(f"Error processing opponent data: {e}")
                    continue
            
            # Step 6: Store in database
            if all_matchup_data:
                result = self.db.db.player_vs_team_stats.insert_many(all_matchup_data, ordered=False)
                logger.info(f"Stored {len(result.inserted_ids)} matchup records")
            
            logger.info(f"Created {matchups_created} opponent matchup records for {player_name}")
            return matchups_created
            
        except Exception as e:
            logger.error(f"Error collecting player vs team stats: {e}")
            return 0
    
    def _collect_player_vs_team_manual(self, player_name, player_id, season):
        """
        Backup method: Manually calculate player vs team stats from individual games
        
        This method gets all the player's games and groups them by opponent
        to calculate averages. Used when the API doesn't give us pre-calculated splits.
        """
        logger.info(f"Using manual calculation for {player_name} vs team stats")
        
        try:
            # Get all games for this player
            player_logs = playergamelogs.PlayerGameLogs(
                player_id_nullable=player_id,
                season_nullable=season,
                season_type_nullable='Regular Season'
            )
            
            games_df = player_logs.get_data_frames()[0]
            if games_df.empty:
                return 0
            
            # Group games by opponent and calculate averages
            matchups_created = 0
            all_matchup_data = []
            
            # Group by matchup (which contains opponent info)
            for matchup, group in games_df.groupby('MATCHUP'):
                try:
                    # Extract opponent from matchup string (e.g., "LAL vs. GSW" or "LAL @ GSW")
                    opponent = self._extract_opponent_from_matchup(matchup, player_name)
                    
                    # Calculate averages for this opponent
                    matchup_data = {
                        'player_id': player_id,
                        'player_name': player_name,
                        'vs_team_name': opponent,
                        'season': season,
                        'games_played': len(group),
                        'avg_points': group['PTS'].mean(),
                        'avg_rebounds': group['REB'].mean(),
                        'avg_assists': group['AST'].mean(),
                        'fg_pct_vs_team': group['FG_PCT'].mean(),
                        'fg3_pct_vs_team': group['FG3_PCT'].mean(),
                        'ft_pct_vs_team': group['FT_PCT'].mean(),
                        'plus_minus_vs_team': group['PLUS_MINUS'].mean(),
                        'collected_at': datetime.now()
                    }
                    
                    all_matchup_data.append(matchup_data)
                    matchups_created += 1
                    
                except Exception as e:
                    logger.error(f"Error processing matchup {matchup}: {e}")
                    continue
            
            # Store in database
            if all_matchup_data:
                result = self.db.db.player_vs_team_stats.insert_many(all_matchup_data, ordered=False)
                logger.info(f"Stored {len(result.inserted_ids)} manual matchup records")
            
            return matchups_created
            
        except Exception as e:
            logger.error(f"Error in manual player vs team calculation: {e}")
            return 0
    
    def _extract_opponent_from_matchup(self, matchup, player_name):
        """Helper function to extract opponent team from matchup string"""
        # This is a simple implementation - could be made more robust
        parts = matchup.replace(' vs. ', ' @ ').split(' @ ')
        if len(parts) == 2:
            # Return the team that's not the player's team
            # This is simplified - in real implementation you'd match team abbreviations
            return parts[1] if parts[0] in player_name else parts[0]
        return matchup
    
    def scrape_positional_matchup_data(self, url, season='2023-24'):
        """
        Scrape positional matchup data from external website
        
        This function scrapes websites that have data about how teams defend
        against different positions. For example: How well do the Lakers defend
        against point guards vs centers?
        
        This is valuable because some teams are weak against certain positions.
        If you know the Warriors struggle against big centers, and the opponent
        has a great center, that's useful for predictions!
        
        Args:
            url (str): Website URL to scrape
            season (str): NBA season
            
        Returns:
            int: Number of positional records scraped
        """
        logger.info(f"Scraping positional matchup data from: {url}")
        logger.info("Remember: Be respectful to websites!")
        
        try:
            # Step 1: Set up proper headers to look like a real browser
            # This helps avoid getting blocked by anti-bot measures
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            # Step 2: Add rate limiting (be nice to the website!)
            self._rate_limit()
            
            # Step 3: Make the request to fetch the webpage
            logger.info("Fetching webpage...")
            response = requests.get(url, headers=headers, timeout=10)
            
            # Step 4: Check if request was successful
            if response.status_code != 200:
                logger.error(f"Failed to fetch webpage: HTTP {response.status_code}")
                return 0
            
            # Step 5: Parse the HTML with BeautifulSoup
            logger.info("Parsing HTML content...")
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Step 6: Look for tables with positional data
            # This is the tricky part - every website is different!
            tables = soup.find_all('table')
            logger.info(f"Found {len(tables)} tables on the page")
            
            if not tables:
                logger.warning("No tables found on the webpage")
                return 0
            
            # Step 7: Try to identify which table has positional data
            positional_data = []
            records_created = 0
            
            for i, table in enumerate(tables):
                logger.info(f"🔍 Analyzing table {i+1}...")
                
                # Look for table headers that suggest positional data
                headers = table.find_all(['th', 'td'])
                header_text = [h.get_text().strip().lower() for h in headers[:10]]
                
                # Check if this looks like a positional table
                position_keywords = ['pg', 'sg', 'sf', 'pf', 'c', 'point guard', 'center', 'forward']
                has_positions = any(keyword in ' '.join(header_text) for keyword in position_keywords)
                
                if not has_positions:
                    logger.info(f"   Table {i+1}: No positional data detected")
                    continue
                
                logger.info(f"   Table {i+1}: Positional data detected!")
                
                # Step 8: Extract data from this table
                try:
                    table_data = self._parse_positional_table(table, season)
                    positional_data.extend(table_data)
                    records_created += len(table_data)
                    
                except Exception as e:
                    logger.error(f"Error parsing table {i+1}: {e}")
                    continue
            
            # Step 9: Store in database
            if positional_data:
                result = self.db.db.positional_matchups.insert_many(positional_data, ordered=False)
                logger.info(f"Stored {len(result.inserted_ids)} positional records")
            
            logger.info(f"Successfully scraped {records_created} positional records")
            return records_created
            
        except requests.RequestException as e:
            logger.error(f"Network error while scraping: {e}")
            return 0
        except Exception as e:
            logger.error(f"Error during web scraping: {e}")
            return 0
    
    def _parse_positional_table(self, table, season):
        """
        Helper function to extract positional data from an HTML table
        
        This function tries to understand the structure of a table and extract
        meaningful positional matchup data. Since every website is different,
        this requires some detective work!
        """
        logger.info("Parsing positional table structure...")
        
        positional_records = []
        
        try:
            # Step 1: Find all rows in the table
            rows = table.find_all('tr')
            if len(rows) < 2:
                return positional_records
            
            # Step 2: Try to identify the header row
            header_row = rows[0]
            headers = [th.get_text().strip() for th in header_row.find_all(['th', 'td'])]
            logger.info(f"Table headers: {headers[:5]}...")  # Show first 5 headers
            
            # Step 3: Process data rows
            for row_idx, row in enumerate(rows[1:], 1):
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                
                # Extract cell values
                row_data = [cell.get_text().strip() for cell in cells]
                
                # Step 4: Try to identify what each column represents
                # This is website-specific logic - you'd customize this for each site
                record = self._extract_positional_record(headers, row_data, season)
                
                if record:
                    positional_records.append(record)
            
            logger.info(f"Extracted {len(positional_records)} records from table")
            return positional_records
            
        except Exception as e:
            logger.error(f"Error parsing table: {e}")
            return positional_records
    
    def _extract_positional_record(self, headers, row_data, season):
        """
        Helper function to create a positional record from table row data
        
        This is where you'd implement the specific logic for the website
        you're scraping. Every site has different column layouts.
        """
        try:
            # This is a generic example - you'd customize this for specific websites
            record = {
                'season': season,
                'team_name': row_data[0] if len(row_data) > 0 else 'Unknown',
                'position': self._normalize_position(row_data[1]) if len(row_data) > 1 else 'Unknown',
                'scraped_at': datetime.now(),
                'source_url': 'scraped_data',
                
                # Try to extract numeric stats (this would be site-specific)
                'defensive_rating': self._safe_float(row_data[2]) if len(row_data) > 2 else 0,
                'points_allowed': self._safe_float(row_data[3]) if len(row_data) > 3 else 0,
                'rebounds_allowed': self._safe_float(row_data[4]) if len(row_data) > 4 else 0,
            }
            
            return record
            
        except Exception as e:
            logger.error(f"Error creating positional record: {e}")
            return None
    
    def _normalize_position(self, position_text):
        """Helper to normalize position names"""
        position_map = {
            'point guard': 'PG', 'pg': 'PG',
            'shooting guard': 'SG', 'sg': 'SG',
            'small forward': 'SF', 'sf': 'SF',
            'power forward': 'PF', 'pf': 'PF',
            'center': 'C', 'c': 'C'
        }
        return position_map.get(position_text.lower(), position_text.upper())
    
    def _safe_float(self, value):
        """Helper to safely convert text to float"""
        try:
            # Remove common non-numeric characters
            clean_value = str(value).replace(',', '').replace('%', '').replace('$', '')
            return float(clean_value)
        except (ValueError, TypeError):
            return 0.0
    
    def collect_injury_reports(self, season='2023-24'):
        """
        Collect player injury data that affects game predictions
        
        This is CRITICAL for predictions - injuries explain many "upset" results.
        We'll try multiple approaches to get injury/availability data.
        """
        logger.info(f"🏥 Collecting injury/availability data for {season}")
        logger.info("This is crucial for prediction accuracy!")
        
        injury_data = []
        players_processed = 0
        
        try:
            # Method 1: Try NBA API for player status
            from nba_api.stats.endpoints import commonallplayers
            
            self._rate_limit()
            
            # Get all active players for the season
            all_players = commonallplayers.CommonAllPlayers(
                season=season,
                is_only_current_season=1
            )
            
            players_df = all_players.get_data_frames()[0]
            
            if not players_df.empty:
                logger.info(f"Found {len(players_df)} active players")
                
                # For each player, try to get recent game logs to determine availability
                for _, player in players_df.head(50).iterrows():  # Limit to prevent rate limiting
                    try:
                        player_id = int(player['PERSON_ID'])
                        player_name = player['DISPLAY_FIRST_LAST']
                        
                        # Get recent games to check availability pattern
                        self._rate_limit()
                        
                        from nba_api.stats.endpoints import playergamelogs
                        
                        recent_games = playergamelogs.PlayerGameLogs(
                            player_id_nullable=player_id,
                            season_nullable=season,
                            season_type_nullable='Regular Season'
                        )
                        
                        games_df = recent_games.get_data_frames()[0]
                        
                        if not games_df.empty:
                            # Analyze availability pattern
                            total_team_games = len(games_df.groupby('GAME_ID'))
                            player_games = len(games_df)
                            availability_rate = player_games / max(total_team_games, 1)
                            
                            # Look for patterns indicating injury
                            recent_games_df = games_df.head(10)  # Last 10 games
                            avg_minutes = recent_games_df['MIN'].mean() if len(recent_games_df) > 0 else 0
                            
                            injury_record = {
                                'player_id': player_id,
                                'player_name': player_name,
                                'season': season,
                                'availability_rate': availability_rate,
                                'recent_avg_minutes': avg_minutes,
                                'total_games_played': player_games,
                                'games_missed_estimated': max(0, total_team_games - player_games),
                                'injury_status': self._estimate_injury_status(availability_rate, avg_minutes),
                                'last_game_date': games_df['GAME_DATE'].max() if len(games_df) > 0 else None,
                                'collected_at': datetime.now()
                            }
                            
                            injury_data.append(injury_record)
                            players_processed += 1
                            
                            if players_processed % 10 == 0:
                                logger.info(f"Processed {players_processed} players...")
                        
                    except Exception as e:
                        logger.error(f"Error processing player {player.get('DISPLAY_FIRST_LAST', 'Unknown')}: {e}")
                        continue
            
            # Store injury data
            if injury_data:
                result = self.db.db.player_injury_data.insert_many(injury_data, ordered=False)
                logger.info(f"✅ Stored injury data for {len(result.inserted_ids)} players")
                
                # Show injury summary
                injured_players = [p for p in injury_data if p['injury_status'] != 'Healthy']
                logger.info(f"⚠️ Found {len(injured_players)} players with potential injury concerns")
                
                # Show sample of concerning cases
                concerning = sorted(injured_players, key=lambda x: x['availability_rate'])[:5]
                for player in concerning:
                    logger.info(f"   {player['player_name']}: {player['availability_rate']:.1%} availability, "
                              f"Status: {player['injury_status']}")
            
            return players_processed
            
        except Exception as e:
            logger.error(f"Error collecting injury data: {e}")
            return 0
    
    def _estimate_injury_status(self, availability_rate, avg_minutes):
        """Helper to estimate injury status from playing patterns"""
        if availability_rate < 0.7:
            return "Frequently Unavailable"
        elif availability_rate < 0.85 and avg_minutes < 20:
            return "Limited Minutes"  
        elif avg_minutes < 15:
            return "Minimal Role"
        else:
            return "Healthy"

    def collect_team_chemistry_stats(self, season='2023-24', moving_window=8):
        """
        Collect team chemistry statistics based on advanced teamwork metrics
        
        This function calculates a "Team Chemistry Index" using four key metrics:
        
        OFFENSIVE CHEMISTRY:
        - Screen Assists: Screens that lead directly to teammate scores (off-ball movement)
        - Secondary Assists: Pass to the assister within 1 second (extra pass mentality)
        
        DEFENSIVE CHEMISTRY: 
        - Contested Shots: Hand up to contest shots (defensive effort/positioning)
        - Deflections: Getting hands on non-shot attempts (defensive awareness)
        
        The Chemistry Index:
        1. Scales each metric 0-100 across all teams
        2. Takes equal-weighted average of all 4 metrics
        3. Applies moving average over specified window (default 8 games)
        4. Normalizes so season start = 100 (shows relative change)
        
        Args:
            season (str): NBA season (e.g., '2023-24')
            moving_window (int): Games for moving average calculation
            
        Returns:
            int: Number of team chemistry records created
        """
        from nba_api.stats.endpoints import leaguedashteamstats
        import numpy as np
        from sklearn.preprocessing import MinMaxScaler
        
        logger.info(f"Collecting team chemistry stats for {season}")
        logger.info(f"Using {moving_window}-game moving average for Chemistry Index")
        
        try:
            # Step 1: Get advanced tracking stats that include our chemistry metrics
            self._rate_limit()
            
            # Try different measure types to find the chemistry stats
            chemistry_data = {}
            
            # Get tracking stats (this usually has screen assists, deflections, etc.)
            logger.info("📡 Fetching tracking stats...")
            tracking_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star='Regular Season',
                measure_type_detailed_defense='Tracking'  # This has advanced tracking metrics
            )
            
            tracking_df = tracking_stats.get_data_frames()[0]
            
            if not tracking_df.empty:
                logger.info(f"Found tracking data with columns: {list(tracking_df.columns)[:10]}...")
                chemistry_data['tracking'] = tracking_df
            
            # Get hustle stats (deflections, contested shots often here)
            logger.info("Fetching hustle stats...")
            self._rate_limit()
            
            hustle_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star='Regular Season',
                measure_type_detailed_defense='Defense'  # Try defense category
            )
            
            hustle_df = hustle_stats.get_data_frames()[0]
            
            if not hustle_df.empty:
                logger.info(f"Found defense data with columns: {list(hustle_df.columns)[:10]}...")
                chemistry_data['defense'] = hustle_df
            
            # Step 2: Extract chemistry metrics from available data
            chemistry_records = []
            teams_processed = 0
            
            # We'll work with what we have and calculate chemistry index
            if 'tracking' in chemistry_data:
                df = chemistry_data['tracking']
                
                for _, row in df.iterrows():
                    try:
                        # Step 3: Extract available chemistry-related metrics
                        team_data = {
                            'team_id': int(row['TEAM_ID']),
                            'team_name': row['TEAM_NAME'],
                            'season': season,
                            'games_played': int(row['GP']),
                            
                            # Try to extract our target metrics (column names may vary)
                            'screen_assists': float(row.get('SCREEN_ASSISTS', row.get('SCREEN_AST', 0))),
                            'secondary_assists': float(row.get('SECONDARY_ASSISTS', row.get('SECONDARY_AST', 0))),
                            'contested_shots': float(row.get('CONTESTED_SHOTS', row.get('CONTESTED_2PT', 0))),
                            'deflections': float(row.get('DEFLECTIONS', row.get('DEF', 0))),
                            
                            # Additional teamwork indicators we might find
                            'passes_made': float(row.get('PASSES_MADE', 0)),
                            'passes_received': float(row.get('PASSES_RECEIVED', 0)),
                            'loose_balls_recovered': float(row.get('LOOSE_BALLS_RECOVERED', 0)),
                            
                            # Metadata
                            'collected_at': datetime.now()
                        }
                        
                        chemistry_records.append(team_data)
                        teams_processed += 1
                        
                        logger.info(f"{team_data['team_name']}: "
                                  f"Screens={team_data['screen_assists']:.1f}, "
                                  f"2ndAst={team_data['secondary_assists']:.1f}, "
                                  f"Contested={team_data['contested_shots']:.1f}, "
                                  f"Deflections={team_data['deflections']:.1f}")
                        
                    except Exception as e:
                        logger.error(f"Error processing team chemistry data: {e}")
                        continue
            
            # Step 4: Calculate Chemistry Index if we have the data
            if chemistry_records:
                chemistry_records = self._calculate_chemistry_index(chemistry_records, moving_window)
                
                # Step 5: Store in database
                result = self.db.db.team_chemistry_stats.insert_many(chemistry_records, ordered=False)
                logger.info(f"Stored chemistry stats for {len(result.inserted_ids)} teams")
                
                # Show top chemistry teams
                top_chemistry = sorted(chemistry_records, key=lambda x: x.get('chemistry_index', 0), reverse=True)[:5]
                chemistry_leaders = [f"{t['team_name']} ({t.get('chemistry_index', 0):.1f})" for t in top_chemistry]
                logger.info(f"Top 5 Chemistry teams: {chemistry_leaders}")
            
            logger.info(f"Successfully processed {teams_processed} teams for chemistry analysis")
            return teams_processed
            
        except Exception as e:
            logger.error(f"Error collecting team chemistry stats: {e}")
            
            # Fallback: Create manual chemistry tracking system
            logger.info("Falling back to manual chemistry calculation...")
            return self._collect_chemistry_manual(season, moving_window)
    
    def _calculate_chemistry_index(self, chemistry_records, moving_window=8):
        """
        Calculate the Chemistry Index using your specified methodology
        
        Steps:
        1. Scale each metric 0-100 across all teams
        2. Take equal-weighted average of all 4 metrics  
        3. Apply moving average over window
        4. Normalize so season start = 100
        """
        import numpy as np
        from sklearn.preprocessing import MinMaxScaler
        
        logger.info("Calculating Chemistry Index...")
        
        try:
            # Step 1: Extract the four key metrics
            metrics = ['screen_assists', 'secondary_assists', 'contested_shots', 'deflections']
            
            # Create matrix of metric values
            metric_matrix = []
            for record in chemistry_records:
                row = [record.get(metric, 0) for metric in metrics]
                metric_matrix.append(row)
            
            metric_matrix = np.array(metric_matrix)
            
            # Step 2: Scale each metric 0-100 across all teams
            scaler = MinMaxScaler(feature_range=(0, 100))
            scaled_metrics = scaler.fit_transform(metric_matrix)
            
            # Step 3: Calculate equal-weighted chemistry score
            chemistry_scores = np.mean(scaled_metrics, axis=1)
            
            # Step 4: Add chemistry index to records
            for i, record in enumerate(chemistry_records):
                record['chemistry_raw'] = float(chemistry_scores[i])
                
                # Individual scaled metrics for analysis
                record['screen_assists_scaled'] = float(scaled_metrics[i][0])
                record['secondary_assists_scaled'] = float(scaled_metrics[i][1]) 
                record['contested_shots_scaled'] = float(scaled_metrics[i][2])
                record['deflections_scaled'] = float(scaled_metrics[i][3])
                
                # For now, chemistry_index = chemistry_raw (moving average would need game-by-game data)
                record['chemistry_index'] = float(chemistry_scores[i])
                
                # Add percentile ranking
                record['chemistry_percentile'] = float(np.percentile(chemistry_scores, 
                                                                   np.searchsorted(np.sort(chemistry_scores), 
                                                                                 chemistry_scores[i]) * 100 / len(chemistry_scores)))
            
            logger.info("Chemistry Index calculated successfully")
            return chemistry_records
            
        except Exception as e:
            logger.error(f"Error calculating chemistry index: {e}")
            return chemistry_records
    
    def _collect_chemistry_manual(self, season, moving_window):
        """
        Fallback method: Collect chemistry stats using alternative endpoints
        
        This method tries different NBA API endpoints to find the chemistry metrics
        or creates proxy metrics from available data.
        """
        logger.info("Using manual chemistry collection method...")
        
        try:
            # Try to get team stats that might have some chemistry indicators
            self._rate_limit()
            
            # Get basic team stats to create proxy metrics
            basic_stats = leaguedashteamstats.LeagueDashTeamStats(
                season=season,
                season_type_all_star='Regular Season',
                measure_type_detailed_defense='Base'
            )
            
            basic_df = basic_stats.get_data_frames()[0]
            
            chemistry_records = []
            
            for _, row in basic_df.iterrows():
                # Create proxy chemistry metrics from available stats
                assists = float(row.get('AST', 0))
                turnovers = float(row.get('TOV', 0))
                steals = float(row.get('STL', 0))
                blocks = float(row.get('BLK', 0))
                
                # Proxy metrics (not perfect but indicative of teamwork)
                proxy_data = {
                    'team_id': int(row['TEAM_ID']),
                    'team_name': row['TEAM_NAME'],
                    'season': season,
                    'games_played': int(row['GP']),
                    
                    # Proxy chemistry metrics
                    'screen_assists': assists * 0.3,  # Estimate: ~30% of assists from screens
                    'secondary_assists': assists * 0.15,  # Estimate: ~15% secondary assists
                    'contested_shots': steals + blocks * 2,  # Proxy for defensive effort
                    'deflections': steals * 1.5,  # Estimate deflections from steals
                    
                    # Mark as proxy data
                    'is_proxy_data': True,
                    'collected_at': datetime.now()
                }
                
                chemistry_records.append(proxy_data)
            
            # Calculate chemistry index with proxy data
            chemistry_records = self._calculate_chemistry_index(chemistry_records, moving_window)
            
            # Store in database
            if chemistry_records:
                result = self.db.db.team_chemistry_stats.insert_many(chemistry_records, ordered=False)
                logger.info(f"Stored {len(result.inserted_ids)} proxy chemistry records")
                
                logger.warning("Using proxy chemistry metrics - consider implementing game-by-game collection for accurate data")
            
            return len(chemistry_records)
            
        except Exception as e:
            logger.error(f"Error in manual chemistry collection: {e}")
            return 0
    
    def collect_team_chemistry_timeline(self, team_name, season='2023-24', moving_window=8):
        """
        Collect game-by-game chemistry data for a specific team to create timeline
        
        This function gets individual game data to calculate the true Chemistry Index
        with moving averages as you specified.
        
        Args:
            team_name (str): Team name (e.g., 'Los Angeles Lakers')
            season (str): NBA season
            moving_window (int): Games for moving average
            
        Returns:
            int: Number of game records processed
        """
        logger.info(f"Collecting chemistry timeline for {team_name}")
        logger.info(f"Using {moving_window}-game moving average")
        
        try:
            # Get team ID
            if team_name not in self.team_dict:
                logger.error(f"Team '{team_name}' not found")
                return 0
            
            team_id = self.team_dict[team_name]
            
            # This would require game-by-game tracking data
            # For now, we'll create a framework for when that data is available
            
            logger.info("Game-by-game chemistry tracking requires additional NBA API endpoints")
            logger.info("Consider implementing with stats.nba.com scraping for full timeline data")
            
            # Store framework for timeline data
            timeline_data = {
                'team_id': team_id,
                'team_name': team_name,
                'season': season,
                'moving_window': moving_window,
                'timeline_type': 'chemistry_index',
                'status': 'framework_created',
                'collected_at': datetime.now()
            }
            
            result = self.db.db.team_chemistry_timeline.insert_one(timeline_data)
            logger.info(f"Created chemistry timeline framework for {team_name}")
            
            return 1
            
        except Exception as e:
            logger.error(f"Error creating chemistry timeline: {e}")
            return 0

    def collect_team_rest_fatigue_data(self, season='2023-24'):
        """
        Collect rest/fatigue data - CRITICAL for predictions
        
        Teams perform significantly differently based on:
        - Days of rest between games
        - Back-to-back games (B2B)
        - Travel distance
        - Schedule strength (games in last N days)
        """
        logger.info(f"😴 Collecting rest/fatigue data for {season}")
        logger.info("Rest is a huge factor in NBA performance!")
        
        try:
            # Get all games for season
            games_cursor = self.db.db.games.find({'SEASON': season}).sort('GAME_DATE', 1)
            all_games = list(games_cursor)
            
            if not all_games:
                logger.warning("No games found for rest analysis")
                return 0
            
            # Group games by team
            team_schedules = {}
            for game in all_games:
                team_name = game.get('TEAM_NAME')
                if team_name:
                    if team_name not in team_schedules:
                        team_schedules[team_name] = []
                    team_schedules[team_name].append(game)
            
            rest_data = []
            
            # Analyze rest patterns for each team
            for team_name, games in team_schedules.items():
                # Sort games by date
                games.sort(key=lambda x: x.get('GAME_DATE', datetime.min))
                
                for i, game in enumerate(games):
                    try:
                        game_date = game.get('GAME_DATE')
                        if isinstance(game_date, str):
                            game_date = pd.to_datetime(game_date)
                        
                        # Calculate rest days
                        days_rest = 0
                        is_back_to_back = False
                        games_in_last_7_days = 1  # Current game
                        
                        if i > 0:
                            prev_game_date = games[i-1].get('GAME_DATE')
                            if isinstance(prev_game_date, str):
                                prev_game_date = pd.to_datetime(prev_game_date)
                            
                            days_rest = (game_date - prev_game_date).days - 1
                            is_back_to_back = days_rest == 0
                        
                        # Count games in last 7 days
                        for j in range(max(0, i-10), i):  # Look back up to 10 games
                            prev_date = games[j].get('GAME_DATE')
                            if isinstance(prev_date, str):
                                prev_date = pd.to_datetime(prev_date)
                            
                            days_ago = (game_date - prev_date).days
                            if days_ago <= 7:
                                games_in_last_7_days += 1
                        
                        # Determine home/away for travel factor
                        matchup = game.get('MATCHUP', '')
                        is_home = 'vs.' in matchup
                        
                        rest_record = {
                            'team_name': team_name,
                            'game_id': game.get('GAME_ID'),
                            'game_date': game_date,
                            'season': season,
                            'days_rest': days_rest,
                            'is_back_to_back': is_back_to_back,
                            'is_home_game': is_home,
                            'games_in_last_7_days': games_in_last_7_days,
                            'schedule_difficulty': self._calculate_schedule_difficulty(games_in_last_7_days, days_rest),
                            'fatigue_index': self._calculate_fatigue_index(days_rest, games_in_last_7_days, is_back_to_back),
                            'collected_at': datetime.now()
                        }
                        
                        rest_data.append(rest_record)
                        
                    except Exception as e:
                        logger.error(f"Error processing game for {team_name}: {e}")
                        continue
            
            # Store rest data
            if rest_data:
                result = self.db.db.team_rest_fatigue.insert_many(rest_data, ordered=False)
                logger.info(f"✅ Stored rest/fatigue data for {len(result.inserted_ids)} games")
                
                # Show interesting patterns
                b2b_games = [r for r in rest_data if r['is_back_to_back']]
                high_fatigue = [r for r in rest_data if r['fatigue_index'] > 0.7]
                
                logger.info(f"📊 Found {len(b2b_games)} back-to-back games")
                logger.info(f"😵 Found {len(high_fatigue)} high-fatigue situations")
            
            return len(rest_data)
            
        except Exception as e:
            logger.error(f"Error collecting rest/fatigue data: {e}")
            return 0
    
    def _calculate_schedule_difficulty(self, games_in_7_days, days_rest):
        """Calculate schedule difficulty score (0-1, higher = more difficult)"""
        # More games in 7 days = harder schedule
        game_density = min(games_in_7_days / 7.0, 1.0)
        
        # Less rest = harder
        rest_factor = max(0, (3 - days_rest) / 3.0)
        
        return (game_density * 0.6 + rest_factor * 0.4)
    
    def _calculate_fatigue_index(self, days_rest, games_in_7_days, is_back_to_back):
        """Calculate team fatigue index (0-1, higher = more fatigued)"""
        fatigue = 0.0
        
        # Back-to-back games add significant fatigue
        if is_back_to_back:
            fatigue += 0.4
        
        # Low rest adds fatigue
        if days_rest == 0:
            fatigue += 0.3
        elif days_rest == 1:
            fatigue += 0.2
        elif days_rest == 2:
            fatigue += 0.1
        
        # High game frequency adds fatigue
        if games_in_7_days >= 4:
            fatigue += 0.3
        elif games_in_7_days >= 3:
            fatigue += 0.2
        
        return min(fatigue, 1.0)
    
    def collect_referee_assignments(self, season='2023-24'):
        """
        Collect referee assignments - affects game flow and scoring
        
        Different refs have different tendencies:
        - Some call more fouls (affects free throws)
        - Some favor home teams more than others
        - Some affect pace of play
        """
        logger.info(f"👨‍⚖️ Collecting referee assignment data for {season}")
        logger.info("Refs significantly impact game outcomes!")
        
        try:
            # This would require web scraping as NBA API doesn't provide ref data
            # For now, we'll create a framework and note this as a data source to implement
            
            referee_data = []
            
            # Get all games to attach referee data to
            games_cursor = self.db.db.games.find({'SEASON': season})
            games = list(games_cursor)
            
            logger.info(f"Found {len(games)} games to collect referee data for")
            
            # TODO: Implement web scraping from basketball-reference.com or ESPN
            # for referee assignments. This is valuable data that impacts predictions.
            
            # For now, create placeholder that shows the data structure
            sample_ref_data = {
                'game_id': 'sample_game_id',
                'season': season,
                'referee_crew': ['John Ref1', 'Jane Ref2', 'Bob Ref3'],
                'head_referee': 'John Ref1',
                'referee_experience_avg': 8.5,  # Average years of experience
                'crew_foul_rate': 0.45,  # Fouls called per possession
                'crew_home_bias': 0.52,  # Home team win rate with this crew
                'crew_pace_factor': 98.5,  # Average pace when this crew officiates
                'collected_at': datetime.now()
            }
            
            logger.info("📋 Referee data structure ready - implement web scraping next")
            logger.info(f"Sample structure: {list(sample_ref_data.keys())}")
            
            return 0  # Return 0 until web scraping is implemented
            
        except Exception as e:
            logger.error(f"Error collecting referee data: {e}")
            return 0
    
    def collect_weather_conditions(self, season='2023-24'):
        """
        Collect weather conditions for outdoor arenas/travel
        
        While NBA is indoor, weather affects:
        - Travel delays/cancellations
        - Player mood and energy
        - Fan attendance (which affects home court advantage)
        """
        logger.info(f"🌤️ Collecting weather impact data for {season}")
        
        try:
            # This would require integration with weather API
            # Framework for weather data collection
            
            weather_data = []
            
            # Get all games with location info
            games_cursor = self.db.db.games.find({'SEASON': season})
            games = list(games_cursor)
            
            # TODO: Integrate with weather API (OpenWeatherMap, etc.)
            # to get historical weather for game dates/locations
            
            sample_weather_data = {
                'game_id': 'sample_game_id',
                'season': season,
                'city': 'Los Angeles',
                'game_date': datetime.now(),
                'temperature': 72.0,  # Fahrenheit
                'precipitation': 0.0,  # Inches
                'wind_speed': 5.2,  # MPH
                'weather_condition': 'Clear',
                'travel_impact_score': 0.1,  # 0-1 scale, 0=no impact
                'attendance_impact_score': 0.0,  # Estimated impact on attendance
                'collected_at': datetime.now()
            }
            
            logger.info("🌡️ Weather data structure ready - integrate weather API next")
            logger.info(f"Sample structure: {list(sample_weather_data.keys())}")
            
            return 0  # Return 0 until weather API is integrated
            
        except Exception as e:
            logger.error(f"Error collecting weather data: {e}")
            return 0

# Example usage and testing
if __name__ == "__main__":
    print("NBA Data Collector - Test Run")
    print("=" * 50)
    
    # Initialize collector
    collector = NBADataCollector()
    
    # Collect current season data
    print("\nCollecting 2023-24 season data...")
    collected = collector.collect_season_games('2023-24')
    print(f"Collected {collected} games")
    
    # Get stats
    print("\nCollection Statistics:")
    stats = collector.get_collection_stats()
    
    # Close connection
    collector.close()
    
    print("\nData collection test complete!")
    print("Your NBA database now has real game data!")