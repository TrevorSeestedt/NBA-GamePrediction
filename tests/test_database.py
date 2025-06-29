#!/usr/bin/env python3
"""
Pytest tests for NBA Database functionality
Tests all database operations with proper fixtures and assertions
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.database import NBADatabase


@pytest.fixture
def db():
    """Database fixture - creates and closes connection for each test"""
    database = NBADatabase()
    yield database
    database.close()


@pytest.fixture
def sample_nba_data():
    """Sample NBA game data for testing"""
    return pd.DataFrame({
        'SEASON_ID': [22023, 22023, 22023],
        'TEAM_ID': [1610612747, 1610612752, 1610612738],  # Lakers, Knicks, Celtics
        'TEAM_ABBREVIATION': ['LAL', 'NYK', 'BOS'],
        'TEAM_NAME': ['Los Angeles Lakers', 'New York Knicks', 'Boston Celtics'],
        'GAME_ID': ['0022301190', '0022301191', '0022301192'],
        'GAME_DATE': [datetime(2024, 4, 14), datetime(2024, 4, 15), datetime(2024, 4, 16)],
        'MATCHUP': ['LAL vs. DEN', 'NYK vs. CHI', 'BOS @ MIA'],
        'WL': ['L', 'W', 'W'],
        'MIN': [240, 265, 240],
        'PTS': [114, 120, 102],
        'FGM': [42, 46, 38],
        'FGA': [88, 91, 85],
        'FG_PCT': [0.477, 0.505, 0.447],
        'FG3M': [15, 12, 18],
        'FG3A': [35, 32, 42],
        'FG3_PCT': [0.429, 0.375, np.nan],  # Test NaN handling
        'FTM': [15, 16, 8],
        'FTA': [18, 21, 10],
        'FT_PCT': [0.833, 0.762, 0.800],
        'OREB': [8, 16, 12],
        'DREB': [35, 37, 32],
        'REB': [43, 53, 44],
        'AST': [30, 27, 25],
        'STL': [8, 7, 9],
        'BLK': [4, 6, 7],
        'TOV': [12, 21, 15],
        'PF': [20, 17, 18],
        'PLUS_MINUS': [-8.0, 1.0, 12.0],
        'SEASON': ['2023-24', '2023-24', '2023-24'],
        'SEASON_TYPE': ['Regular Season', 'Regular Season', 'Regular Season'],
    })


class TestNBADatabase:
    """Test class for NBA Database functionality"""
    
    def test_database_connection(self, db):
        """Test database connection is successful"""
        assert db.client is not None
        assert db.db is not None
        assert db.db.name == "nba_predictor"
    
    def test_insert_games(self, db, sample_nba_data):
        """Test game insertion functionality"""
        # Test successful insertion
        inserted_count = db.insert_games(sample_nba_data)
        assert inserted_count == 3
        
        # Test empty DataFrame
        empty_df = pd.DataFrame()
        inserted_count = db.insert_games(empty_df)
        assert inserted_count == 0
    
    def test_get_games_no_filters(self, db, sample_nba_data):
        """Test retrieving games without filters"""
        # Insert test data first
        db.insert_games(sample_nba_data)
        
        # Retrieve all games
        games = db.get_games()
        assert not games.empty
        assert len(games) >= 3  # At least our test data
        assert '_id' not in games.columns  # MongoDB ID should be removed
    
    def test_get_games_with_filters(self, db, sample_nba_data):
        """Test retrieving games with various filters"""
        # Insert test data
        db.insert_games(sample_nba_data)
        
        # Test team filter
        lakers_games = db.get_games(team_id=1610612747)
        assert not lakers_games.empty
        assert all(lakers_games['TEAM_ID'] == 1610612747)
        
        # Test season filter
        season_games = db.get_games(season='2023-24')
        assert not season_games.empty
        assert all(season_games['SEASON'] == '2023-24')
        
        # Test limit
        limited_games = db.get_games(limit=2)
        assert len(limited_games) <= 2
    
    def test_get_games_date_filters(self, db, sample_nba_data):
        """Test date-based filtering"""
        db.insert_games(sample_nba_data)
        
        # Test start date filter
        start_date = datetime(2024, 4, 15)
        recent_games = db.get_games(start_date=start_date)
        assert not recent_games.empty
        
        # Test end date filter
        end_date = datetime(2024, 4, 15)
        early_games = db.get_games(end_date=end_date)
        assert not early_games.empty
    
    def test_data_type_handling(self, db, sample_nba_data):
        """Test proper handling of different data types"""
        db.insert_games(sample_nba_data)
        games = db.get_games(limit=1)
        
        if not games.empty:
            game = games.iloc[0]
            
            # Test integer fields (only if they exist)
            if 'PTS' in game:
                assert isinstance(game['PTS'], (int, np.integer))
            if 'REB' in game:
                assert isinstance(game['REB'], (int, np.integer))
            
            # Test float fields (only if they exist)
            if 'FG_PCT' in game:
                assert isinstance(game['FG_PCT'], (float, np.floating))
            if 'PLUS_MINUS' in game:
                assert isinstance(game['PLUS_MINUS'], (float, np.floating))
            
            # Test string fields (only if they exist)
            if 'TEAM_ABBREVIATION' in game:
                assert isinstance(game['TEAM_ABBREVIATION'], str)
            if 'WL' in game:
                assert isinstance(game['WL'], str)
    
    def test_nan_handling(self, db, sample_nba_data):
        """Test NaN value handling"""
        db.insert_games(sample_nba_data)
        games = db.get_games(team_id=1610612738)  # Celtics with NaN FG3_PCT
        
        if not games.empty:
            # NaN should be converted to None/null in database
            assert games['FG3_PCT'].isna().any() or games['FG3_PCT'].isnull().any()
    
    def test_create_indexes(self, db):
        """Test index creation"""
        # Should not raise any exceptions
        db.create_indexes()
        
        # Verify indexes exist (basic check)
        indexes = list(db.db.games.list_indexes())
        index_names = [idx['key'] for idx in indexes]
        
        # Check for some key indexes
        assert any('TEAM_ID' in str(idx) for idx in index_names)
        assert any('GAME_DATE' in str(idx) for idx in index_names)
    
    def test_get_team_recent_games(self, db, sample_nba_data):
        """Test retrieving recent games for a team"""
        db.insert_games(sample_nba_data)
        
        # Get recent Lakers games
        recent_games = db.get_team_recent_games(
            team_id=1610612747, 
            before_date=datetime(2024, 4, 20),
            limit=5
        )
        
        # Should return games if any exist
        if not recent_games.empty:
            assert all(recent_games['TEAM_ID'] == 1610612747)
            assert '_id' not in recent_games.columns
    
    def test_get_team_stats(self, db, sample_nba_data):
        """Test team statistics calculation"""
        db.insert_games(sample_nba_data)
        
        # Get Lakers stats
        stats = db.get_team_stats(team_id=1610612747, season='2023-24')
        
        if stats:  # If we have data
            assert 'team_id' in stats
            assert 'total_games' in stats
            assert 'wins' in stats
            assert 'losses' in stats
            assert 'win_percentage' in stats
            assert stats['team_id'] == 1610612747
            assert stats['total_games'] >= 0
            assert 0 <= stats['win_percentage'] <= 1
    
    def test_get_head_to_head(self, db, sample_nba_data):
        """Test head-to-head game retrieval"""
        db.insert_games(sample_nba_data)
        
        # Test head-to-head (may not have data in sample)
        h2h = db.get_head_to_head(
            team1_id=1610612747,  # Lakers
            team2_id=1610612752,  # Knicks
            limit=5
        )
        
        # Should return DataFrame (empty or with data)
        assert isinstance(h2h, pd.DataFrame)
        if not h2h.empty:
            assert '_id' not in h2h.columns


class TestDatabaseEdgeCases:
    """Test edge cases and error handling"""
    
    def test_empty_database_queries(self, db):
        """Test queries on empty database"""
        # Test with non-existent team ID (should return empty)
        games = db.get_games(team_id=999999)
        assert isinstance(games, pd.DataFrame)
        assert games.empty
        
        # Test with non-existent season (should return empty)
        games = db.get_games(season='1999-00')
        assert isinstance(games, pd.DataFrame)
        assert games.empty
        
        stats = db.get_team_stats(team_id=999999)
        assert stats == {}
    
    def test_invalid_team_id(self, db):
        """Test queries with invalid team ID"""
        games = db.get_games(team_id=999999)
        assert games.empty
        
        stats = db.get_team_stats(team_id=999999)
        assert stats == {}
    
    def test_duplicate_insertion(self, db, sample_nba_data):
        """Test duplicate game handling"""
        # Insert same data twice
        first_insert = db.insert_games(sample_nba_data)
        second_insert = db.insert_games(sample_nba_data)
        
        # Second insert should handle duplicates gracefully
        assert isinstance(second_insert, int)
        assert second_insert >= 0  # Should not crash


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"]) 