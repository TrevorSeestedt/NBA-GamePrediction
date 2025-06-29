"""
NBA API Explorer
Let's see what data we can collect!
"""

from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder, teamgamelogs
import pandas as pd
from datetime import datetime
import time

def explore_nba_teams():
    """See what NBA teams are available"""
    print("🏀 NBA Teams Available:")
    print("=" * 50)
    
    nba_teams = teams.get_teams()
    
    print(f"Total teams: {len(nba_teams)}")
    print("\nSample teams:")
    
    for i, team in enumerate(nba_teams[:5]):
        print(f"  {team['full_name']} (ID: {team['id']})")
    
    # Show Lakers and Warriors specifically
    lakers = [team for team in nba_teams if 'Lakers' in team['full_name']][0]
    warriors = [team for team in nba_teams if 'Warriors' in team['full_name']][0]
    
    print(f"\n🏆 Lakers: {lakers}")
    print(f"🏆 Warriors: {warriors}")
    
    return nba_teams

def explore_game_data():
    """See what game data looks like"""
    print("\n📊 Sample Game Data:")
    print("=" * 50)
    
    try:
        # Get a small sample of recent games
        print("Fetching sample games...")
        time.sleep(1)  # Rate limiting
        
        gamefinder = leaguegamefinder.LeagueGameFinder(
            season_nullable='2023-24',
            season_type_nullable='Regular Season'
        )
        
        games = gamefinder.get_data_frames()[0]
        
        print(f"✅ Found {len(games)} game records")
        print(f"Date range: {games['GAME_DATE'].min()} to {games['GAME_DATE'].max()}")
        
        # Show sample data structure
        print("\n📋 Available columns:")
        for i, col in enumerate(games.columns):
            print(f"  {i+1:2d}. {col}")
        
        # Show sample game
        print("\n🎯 Sample game record:")
        sample_game = games.iloc[0]
        for key, value in sample_game.items():
            print(f"  {key}: {value}")
            
        return games
        
    except Exception as e:
        print(f"❌ Error fetching games: {e}")
        return None

def explore_team_games():
    """See what individual team data looks like"""
    print("\n🏀 Team-Specific Game Data:")
    print("=" * 50)
    
    try:
        # Get Lakers games as example
        lakers_id = 1610612747
        print(f"Fetching Lakers games (ID: {lakers_id})...")
        time.sleep(1)
        
        team_games = teamgamelogs.TeamGameLogs(
            team_id_nullable=lakers_id,
            season_nullable='2023-24',
            season_type_nullable='Regular Season'
        )
        
        games = team_games.get_data_frames()[0]
        
        print(f"✅ Found {len(games)} Lakers games")
        
        # Show wins/losses
        wins = len(games[games['WL'] == 'W'])
        losses = len(games[games['WL'] == 'L'])
        print(f"📊 Record: {wins}-{losses}")
        
        # Show scoring stats
        avg_points = games['PTS'].mean()
        print(f"📊 Average points: {avg_points:.1f}")
        
        return games
        
    except Exception as e:
        print(f"❌ Error fetching team games: {e}")
        return None

def main():
    """Explore NBA API capabilities"""
    print("🔍 NBA API Explorer")
    print("Let's see what data we can collect for your predictor!")
    
    # Explore teams
    teams_data = explore_nba_teams()
    
    # Explore game data
    games_data = explore_game_data()
    
    # Explore team-specific data
    team_data = explore_team_games()
    
    print("\n" + "=" * 60)
    print("🎯 Data Collection Strategy:")
    print("✅ We can get all NBA teams")
    print("✅ We can get historical game data")
    print("✅ We can get team-specific statistics")
    print("✅ Data includes: scores, stats, dates, win/loss")
    
    print("\n🚀 Ready to build the data collector!")
    print("   📡 Collect games from multiple seasons")
    print("   💾 Store in your MongoDB database")
    print("   📊 Build dataset for machine learning")

if __name__ == "__main__":
    main() 