"""
NBA Data Collector Module
Fetches data from NBA API and stores in MongoDB
"""

from nba_api.stats.endpoints import leaguegamefinder, teamgamelogs
from nba_api.stats.static import teams
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from .database import NBADatabase

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBADataCollector:
    def __init__(self, rate_limit_delay=1.5):
        """
        Initialize the NBA data collector
        
        Args:
            rate_limit_delay (float): Seconds to wait between API calls
        """
        self.rate_limit_delay = rate_limit_delay
        self.teams = teams.get_teams()
        self.team_dict = {team['full_name']: team['id'] for team in self.teams}
        
        # Initialize database connection
        self.db = NBADatabase()
        
        logger.info(f"🏀 NBA Data Collector initialized")
        logger.info(f"📊 Found {len(self.teams)} NBA teams")
        logger.info(f"⏱️ Rate limit: {rate_limit_delay}s between requests")
    
    def _rate_limit(self):
        """Apply rate limiting between API calls"""
        time.sleep(self.rate_limit_delay)
    
    def collect_season_games(self, season='2023-24', season_type='Regular Season'):
        """
        Collect all games for a specific season
        
        Args:
            season (str): NBA season (e.g., '2023-24')
            season_type (str): 'Regular Season' or 'Playoffs'
            
        Returns:
            int: Number of games collected
        """
        logger.info(f"🔄 Starting collection for {season} {season_type}")
        
        try:
            # Apply rate limiting
            self._rate_limit()
            
            # Fetch games from NBA API
            logger.info("📡 Fetching games from NBA API...")
            gamefinder = leaguegamefinder.LeagueGameFinder(
                season_nullable=season,
                season_type_nullable=season_type
            )
            
            games_df = gamefinder.get_data_frames()[0]
            
            if games_df.empty:
                logger.warning(f"⚠️ No games found for {season} {season_type}")
                return 0
            
            # Add season and season type to the data
            games_df['SEASON'] = season
            games_df['SEASON_TYPE'] = season_type
            
            # Convert date strings to datetime objects
            games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
            
            logger.info(f"✅ Fetched {len(games_df)} game records from NBA API")
            logger.info(f"📅 Date range: {games_df['GAME_DATE'].min()} to {games_df['GAME_DATE'].max()}")
            
            # Store in MongoDB
            logger.info("💾 Storing games in MongoDB...")
            inserted_count = self.db.insert_games(games_df)
            
            logger.info(f"🎉 Successfully collected {inserted_count} new games for {season}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"❌ Error collecting games for {season}: {e}")
            return 0
    
    def collect_multiple_seasons(self, seasons=['2023-24', '2022-23'], season_type='Regular Season'):
        """
        Collect games for multiple seasons
        
        Args:
            seasons (list): List of seasons to collect
            season_type (str): Type of season
            
        Returns:
            dict: Results for each season
        """
        logger.info(f"🚀 Starting multi-season collection")
        logger.info(f"📋 Seasons: {seasons}")
        
        results = {}
        total_collected = 0
        
        for i, season in enumerate(seasons):
            logger.info(f"📊 Progress: {i+1}/{len(seasons)} - Collecting {season}")
            
            collected = self.collect_season_games(season, season_type)
            results[season] = collected
            total_collected += collected
            
            # Progress update
            logger.info(f"✅ {season}: {collected} games collected")
            
            # Rate limiting between seasons
            if i < len(seasons) - 1:  # Don't wait after the last season
                logger.info(f"⏳ Waiting {self.rate_limit_delay}s before next season...")
                self._rate_limit()
        
        logger.info(f"🎉 Multi-season collection complete!")
        logger.info(f"📊 Total games collected: {total_collected}")
        logger.info(f"📋 Results by season: {results}")
        
        return results
    
    def collect_team_games(self, team_name, season='2023-24', season_type='Regular Season'):
        """
        Collect games for a specific team
        
        Args:
            team_name (str): Full team name (e.g., 'Los Angeles Lakers')
            season (str): NBA season
            season_type (str): Season type
            
        Returns:
            int: Number of games collected
        """
        if team_name not in self.team_dict:
            logger.error(f"❌ Team '{team_name}' not found")
            available_teams = list(self.team_dict.keys())[:5]
            logger.info(f"💡 Available teams (sample): {available_teams}")
            return 0
        
        team_id = self.team_dict[team_name]
        logger.info(f"🏀 Collecting games for {team_name} (ID: {team_id})")
        
        try:
            self._rate_limit()
            
            team_games = teamgamelogs.TeamGameLogs(
                team_id_nullable=team_id,
                season_nullable=season,
                season_type_nullable=season_type
            )
            
            games_df = team_games.get_data_frames()[0]
            
            if games_df.empty:
                logger.warning(f"⚠️ No games found for {team_name}")
                return 0
            
            # Add metadata
            games_df['SEASON'] = season
            games_df['SEASON_TYPE'] = season_type
            games_df['GAME_DATE'] = pd.to_datetime(games_df['GAME_DATE'])
            
            # Store in database
            inserted_count = self.db.insert_games(games_df)
            
            logger.info(f"✅ Collected {inserted_count} games for {team_name}")
            return inserted_count
            
        except Exception as e:
            logger.error(f"❌ Error collecting games for {team_name}: {e}")
            return 0
    
    def get_collection_stats(self):
        """
        Get statistics about collected data
        
        Returns:
            dict: Collection statistics
        """
        logger.info("📊 Gathering collection statistics...")
        
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
            
            logger.info(f"📊 Collection Stats:")
            logger.info(f"   Total games: {stats['total_games']}")
            logger.info(f"   Seasons: {stats['seasons']}")
            logger.info(f"   Teams: {stats['teams']}")
            logger.info(f"   Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}")
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error getting collection stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.db:
            self.db.close()
            logger.info("🔌 Database connection closed")

# Example usage and testing
if __name__ == "__main__":
    print("🏀 NBA Data Collector - Test Run")
    print("=" * 50)
    
    # Initialize collector
    collector = NBADataCollector()
    
    # Collect current season data
    print("\n📡 Collecting 2023-24 season data...")
    collected = collector.collect_season_games('2023-24')
    print(f"✅ Collected {collected} games")
    
    # Get stats
    print("\n📊 Collection Statistics:")
    stats = collector.get_collection_stats()
    
    # Close connection
    collector.close()
    
    print("\n🎉 Data collection test complete!")
    print("💡 Your NBA database now has real game data!")