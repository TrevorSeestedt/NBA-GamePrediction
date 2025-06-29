#!/usr/bin/env python3
"""
Test file for Advanced NBA Data Collection Features
Use this to experiment and test your implementations!
"""

import pytest
import sys
import os

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_collector import NBADataCollector


@pytest.fixture
def collector():
    """Data collector fixture for testing"""
    collector = NBADataCollector(rate_limit_delay=0.1)
    yield collector
    collector.close()


class TestAdvancedDataCollection:
    """Test advanced data collection features"""
    
    def test_team_advanced_stats_structure(self, collector):
        """Test the team advanced stats method structure"""
        # This should work once you implement the method
        result = collector.collect_team_advanced_stats('2023-24')
        
        # For now, just check it returns a number
        assert isinstance(result, int)
        assert result >= 0
    
    def test_player_vs_team_stats_structure(self, collector):
        """Test player vs team stats method structure"""
        # This should work once you implement the method
        result = collector.collect_player_vs_team_stats('LeBron James', '2023-24')
        
        # For now, just check it returns a number
        assert isinstance(result, int)
        assert result >= 0
    
    def test_scraping_method_structure(self, collector):
        """Test web scraping method structure"""
        # Test with a dummy URL
        result = collector.scrape_positional_matchup_data('https://example.com', '2023-24')
        
        # For now, just check it returns a number
        assert isinstance(result, int)
        assert result >= 0


class TestImplementationChallenges:
    """Challenges to guide your implementation"""
    
    def test_challenge_1_team_advanced_stats(self, collector):
        """
        🎯 CHALLENGE 1: Implement collect_team_advanced_stats()
        
        Your Goal: Collect offensive/defensive ratings for all NBA teams
        
        Steps to complete:
        1. Research NBA API endpoints for advanced team stats
        2. Import the correct endpoint (hint: teamdashboardbygeneralsplits)
        3. Loop through all teams and collect their advanced stats
        4. Store the data in your database
        
        Success Criteria:
        - Method returns number > 0 (teams processed)
        - Data includes off_rating, def_rating, pace
        - Data is stored in database
        """
        print("\n🎯 CHALLENGE 1: Team Advanced Stats")
        print("=" * 50)
        print("📋 TODO List:")
        print("   1. Import teamdashboardbygeneralsplits endpoint")
        print("   2. Loop through self.teams")
        print("   3. Collect OffRtg, DefRtg, Pace for each team")
        print("   4. Store in database with proper structure")
        print("   5. Return count of teams processed")
        
        # Uncomment when you're ready to test your implementation
        # result = collector.collect_team_advanced_stats('2023-24')
        # assert result > 0, "Should process at least 1 team"
        
        print("💡 Start by researching the NBA API documentation!")
    
    def test_challenge_2_player_vs_team_performance(self, collector):
        """
        🎯 CHALLENGE 2: Implement collect_player_vs_team_stats()
        
        Your Goal: Find how LeBron James performs against each NBA team
        
        Steps to complete:
        1. Use players.find_players_by_full_name() to get player ID
        2. Research PlayerDashboardByGeneralSplits endpoint
        3. Get stats split by opponent team
        4. Process and store the matchup data
        
        Success Criteria:
        - Method finds the correct player
        - Returns matchup stats vs different teams
        - Data includes points, rebounds, assists vs each team
        """
        print("\n🎯 CHALLENGE 2: Player vs Team Stats")
        print("=" * 50)
        print("📋 TODO List:")
        print("   1. Find LeBron James' player ID")
        print("   2. Import PlayerDashboardByGeneralSplits")
        print("   3. Get his stats split by opponent")
        print("   4. Process matchup data")
        print("   5. Store in database")
        
        # Uncomment when you're ready to test
        # result = collector.collect_player_vs_team_stats('LeBron James', '2023-24')
        # assert result > 0, "Should find matchup data"
        
        print("💡 Start with players.find_players_by_full_name('LeBron James')")
    
    def test_challenge_3_web_scraping(self, collector):
        """
        🎯 CHALLENGE 3: Implement scrape_positional_matchup_data()
        
        Your Goal: Scrape positional matchup data from a website
        
        Steps to complete:
        1. Choose a website with positional NBA data
        2. Use requests to fetch the webpage
        3. Use BeautifulSoup to parse HTML tables
        4. Extract positional matchup statistics
        5. Store in database
        
        Success Criteria:
        - Successfully fetches webpage
        - Parses HTML tables correctly
        - Extracts positional data (PG vs PG, etc.)
        - Handles errors gracefully
        """
        print("\n🎯 CHALLENGE 3: Web Scraping Positional Data")
        print("=" * 50)
        print("📋 TODO List:")
        print("   1. Choose a website (Basketball Reference, ESPN, etc.)")
        print("   2. Inspect the HTML structure")
        print("   3. Use requests.get() to fetch data")
        print("   4. Use BeautifulSoup to parse tables")
        print("   5. Extract positional matchup stats")
        
        print("💡 What website do you want to scrape? Share the URL!")
        print("⚠️  Remember to check robots.txt and be respectful!")


def run_implementation_guide():
    """Guide for implementing the advanced features"""
    print("🏀 NBA Advanced Data Collection - Implementation Guide")
    print("=" * 60)
    
    print("\n🎯 YOUR CHALLENGES:")
    print("1. Team Advanced Stats (Offensive/Defensive Ratings)")
    print("2. Player vs Team Performance")
    print("3. Web Scraping Positional Data")
    
    print("\n📚 RESOURCES TO HELP YOU:")
    print("• NBA API Documentation: https://github.com/swar/nba_api")
    print("• BeautifulSoup Docs: https://www.crummy.com/software/BeautifulSoup/")
    print("• Basketball Reference: https://www.basketball-reference.com/")
    
    print("\n🔧 IMPLEMENTATION TIPS:")
    print("• Start with Challenge 1 (easiest)")
    print("• Test each method individually")
    print("• Use print() statements to debug API responses")
    print("• Handle errors gracefully")
    
    print("\n🚀 READY TO START?")
    print("Run: pytest tests/test_advanced_collection.py -v")


if __name__ == "__main__":
    run_implementation_guide() 