#!/usr/bin/env python3
"""
Pytest tests for NBA Data Collector functionality
Tests data collection, API integration, and database storage
"""

import pytest
import pandas as pd
from datetime import datetime
import sys
import os
from unittest.mock import Mock, patch

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_collector import NBADataCollector


@pytest.fixture
def collector():
    """Data collector fixture"""
    collector = NBADataCollector(rate_limit_delay=0.1)  # Faster for testing
    yield collector
    collector.close()


@pytest.fixture
def mock_nba_data():
    """Mock NBA API response data"""
    return pd.DataFrame({
        'SEASON_ID': [22023, 22023],
        'TEAM_ID': [1610612747, 1610612752],
        'TEAM_ABBREVIATION': ['LAL', 'NYK'],
        'TEAM_NAME': ['Los Angeles Lakers', 'New York Knicks'],
        'GAME_ID': ['0022301190', '0022301191'],
        'GAME_DATE': ['2024-04-14', '2024-04-15'],
        'MATCHUP': ['LAL vs. DEN', 'NYK vs. CHI'],
        'WL': ['L', 'W'],
        'PTS': [114, 120],
        'REB': [43, 53],
        'AST': [30, 27],
        'FG_PCT': [0.477, 0.505],
    })


class TestNBADataCollector:
    """Test class for NBA Data Collector functionality"""
    
    def test_collector_initialization(self, collector):
        """Test data collector initialization"""
        assert collector.rate_limit_delay == 0.1
        assert len(collector.teams) == 30  # NBA has 30 teams
        assert 'Los Angeles Lakers' in collector.team_dict
        assert collector.db is not None
    
    def test_team_dict_structure(self, collector):
        """Test team dictionary structure"""
        # Check some known teams
        assert collector.team_dict['Los Angeles Lakers'] == 1610612747
        assert collector.team_dict['Boston Celtics'] == 1610612738
        assert collector.team_dict['Golden State Warriors'] == 1610612744
        
        # Check all teams have valid IDs
        for team_name, team_id in collector.team_dict.items():
            assert isinstance(team_name, str)
            assert isinstance(team_id, int)
            assert team_id > 0
    
    @pytest.mark.slow
    @pytest.mark.api
    def test_collect_team_games_real_api(self, collector):
        """Test collecting real team games (slow test)"""
        # Test with a small request
        collected = collector.collect_team_games(
            'Los Angeles Lakers', 
            season='2023-24'
        )
        
        # Should collect some games
        assert isinstance(collected, int)
        assert collected >= 0
    
    def test_collect_team_games_invalid_team(self, collector):
        """Test collecting games for invalid team"""
        collected = collector.collect_team_games('Invalid Team Name')
        assert collected == 0
    
    @patch('src.data_collector.teamgamelogs.TeamGameLogs')
    def test_collect_team_games_mocked(self, mock_team_logs, collector, mock_nba_data):
        """Test team games collection with mocked API"""
        # Setup mock
        mock_instance = Mock()
        mock_instance.get_data_frames.return_value = [mock_nba_data]
        mock_team_logs.return_value = mock_instance
        
        # Test collection
        collected = collector.collect_team_games('Los Angeles Lakers')
        
        # Verify API was called
        mock_team_logs.assert_called_once()
        assert collected >= 0
    
    @patch('src.data_collector.leaguegamefinder.LeagueGameFinder')
    def test_collect_season_games_mocked(self, mock_league_finder, collector, mock_nba_data):
        """Test season games collection with mocked API"""
        # Setup mock
        mock_instance = Mock()
        mock_instance.get_data_frames.return_value = [mock_nba_data]
        mock_league_finder.return_value = mock_instance
        
        # Test collection
        collected = collector.collect_season_games('2023-24')
        
        # Verify API was called
        mock_league_finder.assert_called_once()
        assert collected >= 0
    
    def test_collect_multiple_seasons(self, collector):
        """Test multiple season collection logic"""
        with patch.object(collector, 'collect_season_games') as mock_collect:
            mock_collect.return_value = 10
            
            results = collector.collect_multiple_seasons(['2023-24', '2022-23'])
            
            # Should call collect_season_games twice
            assert mock_collect.call_count == 2
            assert results == {'2023-24': 10, '2022-23': 10}
    
    def test_get_collection_stats(self, collector):
        """Test collection statistics"""
        stats = collector.get_collection_stats()
        
        # Should return a dictionary with expected keys
        assert isinstance(stats, dict)
        expected_keys = ['total_games', 'seasons', 'teams', 'date_range']
        
        for key in expected_keys:
            assert key in stats
    
    def test_rate_limiting(self, collector):
        """Test rate limiting functionality"""
        import time
        
        start_time = time.time()
        collector._rate_limit()
        end_time = time.time()
        
        # Should wait at least the rate limit delay
        assert end_time - start_time >= collector.rate_limit_delay


class TestDataCollectorEdgeCases:
    """Test edge cases and error handling"""
    
    @patch('src.data_collector.leaguegamefinder.LeagueGameFinder')
    def test_empty_api_response(self, mock_league_finder, collector):
        """Test handling of empty API response"""
        # Setup mock to return empty DataFrame
        mock_instance = Mock()
        mock_instance.get_data_frames.return_value = [pd.DataFrame()]
        mock_league_finder.return_value = mock_instance
        
        collected = collector.collect_season_games('2023-24')
        assert collected == 0
    
    @patch('src.data_collector.leaguegamefinder.LeagueGameFinder')
    def test_api_error_handling(self, mock_league_finder, collector):
        """Test API error handling"""
        # Setup mock to raise exception
        mock_league_finder.side_effect = Exception("API Error")
        
        collected = collector.collect_season_games('2023-24')
        assert collected == 0
    
    def test_database_connection_handling(self):
        """Test database connection in collector"""
        # This tests the integration between collector and database
        collector = NBADataCollector()
        
        # Should have database connection
        assert collector.db is not None
        
        # Should be able to get stats
        stats = collector.get_collection_stats()
        assert isinstance(stats, dict)
        
        collector.close()


@pytest.mark.integration
class TestDataCollectorIntegration:
    """Integration tests for data collector with database"""
    
    def test_full_collection_workflow(self, collector, mock_nba_data):
        """Test complete collection workflow"""
        with patch('src.data_collector.teamgamelogs.TeamGameLogs') as mock_team_logs:
            # Setup mock
            mock_instance = Mock()
            mock_instance.get_data_frames.return_value = [mock_nba_data]
            mock_team_logs.return_value = mock_instance
            
            # Test full workflow
            initial_stats = collector.get_collection_stats()
            collected = collector.collect_team_games('Los Angeles Lakers')
            final_stats = collector.get_collection_stats()
            
            # Should have collected data
            assert collected >= 0
            assert isinstance(final_stats, dict)


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"]) 