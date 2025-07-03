"""
MongoDB Database Module
Handling all the database ooperations for the NBA data. 
"""
import pymongo
import pandas as pd 
import datetime
import logging
import time

# Logging after imports for early error catching, consistent logging, troubleshooting
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NBADatabase:
    def __init__(self, connection_string="mongodb://admin:password123@localhost:27017/", db_name="nba_predictor"):
        """
        Initialize the database connection
        
        Args:
            connection_string (str): Where to find MongoDB
            db_name (str): Name of our NBA database
        """

        self.connection_string = connection_string # Connection string to the MongoDB Server, URI that specifies the server & port 
        self.db_name = db_name # Name of the NBA database
        self.client = None # MongoDB client
        self.db = None # MongoDB database
        self.connect()

    
    def connect(self):
        """Connect to MongoDB"""
        try:
            # Create the MongoDB client
            self.client = pymongo.MongoClient(self.connection_string)
            
            # Test the connection
            self.client.admin.command('ping')
            
            # Get your database
            self.db = self.client[self.db_name]
            
            # Step 4: Success message
            logging.info(f"✅ Connected to MongoDB database: {self.db_name}")
            
        # Timeout Error
        except pymongo.errors.ServerSelectionTimeoutError:
            # This happens when MongoDB isn't running
            logging.error("MongoDB connection failed: Server not reachable")
            raise
        # Connection Failure
        except pymongo.errors.ConnectionFailure:
            # This happens when there's a network issue
            logging.error("MongoDB connection failed: Connection error")
            raise
        # Unexpected Error
        except Exception as e:
            # This catches any other unexpected errors
            logging.error(f"Unexpected database error: {e}")
            raise

    def connect_with_retry(self, max_retries=3):
        """Connect with retry logic"""
        for attempt in range(max_retries):
            try:
                self.connect()
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff

    def insert_games(self, games_df):
        """
        Insert NBA games into MongoDB
        
        Args:
            games_df (pd.DataFrame): DataFrame with NBA games
            
        Returns:
            int: Number of games inserted
        """
        # 1. Check if games_df is empty
        if games_df.empty:
            logging.warning("Games DataFrame is empty.")
            return 0
        
        # 2. Convert DataFrame to list of dictionaries
        games_list = games_df.to_dict(orient="records")
        
        # 3. Add metadata (like insertion timestamp)
        for game in games_list:
            game["inserted_at"] = datetime.datetime.now()
            # Handle pandas/numpy data types for MongoDB
            for key, value in game.items():
                if pd.isna(value):
                    game[key] = None

        # 4. Insert into MongoDB with duplicate handling
        try:
            result = self.db.games.insert_many(games_list, ordered=False)
            logging.info(f"Inserted {len(result.inserted_ids)} number of games into MongoDB")
            return len(result.inserted_ids)
        
        except pymongo.errors.BulkWriteError as e:
            # Handle duplicates gracefully
            inserted_count = e.details.get('nInserted', 0)
            logging.warning(f"Duplicates found. Inserted {inserted_count} new games")
            return inserted_count
        
        except Exception as e:
            logging.error(f"Error while inserting games: {e}")
            raise
    
    def get_games(self, team_id=None, season=None, start_date=None, end_date=None, limit=None):
        """Retrieve games from MongoDB with filters"""
        # Step 1: Build the query dictionary
        query = {}
        
        # Add filters only if provided
        if team_id:
            query['TEAM_ID'] = team_id
        
        if season: 
            query['SEASON'] = season
        
        if start_date or end_date:
            date_query = {}
            if start_date:
                date_query['$gte'] = start_date  # Greater than or equal
            if end_date:
                date_query['$lte'] = end_date    # Less than or equal
            query['GAME_DATE'] = date_query
        
        # Step 2: Execute the query
        try:
            cursor = self.db.games.find(query)
            
            if limit:  # Apply limit if specified
                cursor = cursor.limit(limit)
            
            games_list = list(cursor)
            
            # Step 3: Handle empty results
            if not games_list:
                logging.info("No games found matching criteria")
                return pd.DataFrame()
            
            # Convert to DataFrame and clean up
            df = pd.DataFrame(games_list)
            
            # Remove MongoDB's _id field (not needed for analysis)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            logging.info(f"Retrieved {len(df)} games from database")
            return df
            
        except Exception as e:
            logging.error(f"Error retrieving games: {e}")
            raise
    
    def create_indexes(self):
        """Create indexes for better query performance"""
        try:
            # Basic indexes for commonly queried fields
            self.db.games.create_index([("TEAM_ID", pymongo.ASCENDING)])
            logging.info("Created index on TEAM_ID")
            
            self.db.games.create_index([("GAME_DATE", pymongo.DESCENDING)])
            logging.info("Created index on GAME_DATE")
            
            self.db.games.create_index([("SEASON", pymongo.ASCENDING)])
            logging.info("Created index on SEASON")
            
            # Additional indexes for NBA-specific queries
            self.db.games.create_index([("GAME_ID", pymongo.ASCENDING)])
            logging.info("Created index on GAME_ID")
            
            self.db.games.create_index([("WL", pymongo.ASCENDING)])
            logging.info("Created index on WL (Win/Loss)")
            
            self.db.games.create_index([("SEASON_TYPE", pymongo.ASCENDING)])
            logging.info("Created index on SEASON_TYPE")
            
            # Compound indexes for common NBA queries
            self.db.games.create_index([
                ("TEAM_ID", pymongo.ASCENDING), 
                ("GAME_DATE", pymongo.DESCENDING)
            ])
            logging.info("Created compound index on TEAM_ID + GAME_DATE")
            
            self.db.games.create_index([
                ("SEASON", pymongo.ASCENDING),
                ("SEASON_TYPE", pymongo.ASCENDING),
                ("TEAM_ID", pymongo.ASCENDING)
            ])
            logging.info("Created compound index on SEASON + SEASON_TYPE + TEAM_ID")
            
            # Index for matchup analysis
            self.db.games.create_index([("MATCHUP", pymongo.ASCENDING)])
            logging.info("Created index on MATCHUP")
            
            logging.info("All database indexes created successfully")
            
        except Exception as e:
            logging.error(f"Error creating indexes: {e}")
            raise
    
    def get_team_recent_games(self, team_id, before_date, limit=10):
        """
        Get a team's recent games before a specific date
        (This will be used to calculate team form)
        """
        try:
            query = {
                "TEAM_ID": team_id, 
                "GAME_DATE": {"$lt": before_date}
            }
            
            cursor = self.db.games.find(query).sort("GAME_DATE", pymongo.DESCENDING).limit(limit)
            games_list = list(cursor)
            
            if not games_list:
                logging.info(f"No recent games found for team {team_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(games_list)
            
            # Remove MongoDB _id field
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            logging.info(f"Retrieved {len(df)} recent games for team {team_id}")
            return df
            
        except Exception as e:
            logging.error(f"Error getting recent games: {e}")
            raise
    
    def get_team_stats(self, team_id, season=None, season_type='Regular Season'):
        """
        Get aggregated statistics for a team
        
        Args:
            team_id (int): Team ID
            season (str): NBA season (optional)
            season_type (str): Season type
            
        Returns:
            dict: Team statistics
        """
        try:
            query = {"TEAM_ID": team_id, "SEASON_TYPE": season_type}
            if season:
                query["SEASON"] = season
            
            games = list(self.db.games.find(query))
            
            if not games:
                return {}
            
            # Calculate basic stats
            total_games = len(games)
            wins = sum(1 for game in games if game.get('WL') == 'W')
            losses = total_games - wins
            
            # Average stats
            avg_pts = sum(game.get('PTS', 0) for game in games) / total_games
            avg_reb = sum(game.get('REB', 0) for game in games) / total_games
            avg_ast = sum(game.get('AST', 0) for game in games) / total_games
            
            stats = {
                'team_id': team_id,
                'season': season,
                'total_games': total_games,
                'wins': wins,
                'losses': losses,
                'win_percentage': wins / total_games if total_games > 0 else 0,
                'avg_points': round(avg_pts, 1),
                'avg_rebounds': round(avg_reb, 1),
                'avg_assists': round(avg_ast, 1)
            }
            
            logging.info(f"Retrieved stats for team {team_id}: {wins}-{losses} record")
            return stats
            
        except Exception as e:
            logging.error(f"Error getting team stats: {e}")
            return {}
    
    def get_head_to_head(self, team1_id, team2_id, season=None, limit=10):
        """
        Get head-to-head matchup history between two teams
        
        Args:
            team1_id (int): First team ID
            team2_id (int): Second team ID  
            season (str): Season filter (optional)
            limit (int): Max number of games to return
            
        Returns:
            pd.DataFrame: Head-to-head game history
        """
        try:
            # Build matchup patterns
            matchup1 = f"% vs. %"  # We'll use regex for flexible matching
            
            query = {
                "$or": [
                    {"TEAM_ID": team1_id, "MATCHUP": {"$regex": str(team2_id)}},
                    {"TEAM_ID": team2_id, "MATCHUP": {"$regex": str(team1_id)}}
                ]
            }
            
            if season:
                query["SEASON"] = season
            
            cursor = self.db.games.find(query).sort("GAME_DATE", pymongo.DESCENDING)
            
            if limit:
                cursor = cursor.limit(limit * 2)  # Get more since we have both teams
            
            games_list = list(cursor)
            
            if not games_list:
                logging.info(f"No head-to-head games found between teams {team1_id} and {team2_id}")
                return pd.DataFrame()
            
            df = pd.DataFrame(games_list)
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            logging.info(f"Retrieved {len(df)} head-to-head games")
            return df
            
        except Exception as e:
            logging.error(f"Error getting head-to-head data: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close database connection"""
        if self.client:
            self.client.close()
            logging.info("Database connection closed.")
        else:
            logging.info("No database connection to close.") 


 