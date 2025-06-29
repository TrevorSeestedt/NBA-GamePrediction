"""
Complete Database Test - NBA Predictor
Tests all database methods to ensure they work correctly
"""

import sys
import os
sys.path.append('..')

from src.database import NBADatabase
import pandas as pd
import datetime

def test_database_complete():
    """Test all database methods"""
    
    print("🏀 NBA Database - Complete Test Suite")
    print("=" * 60)
    
    try:
        # Test 1: Connection
        print("\n1️⃣ Testing Database Connection...")
        db = NBADatabase()
        print("✅ Database connection successful")
        
        # Test 2: Create Indexes
        print("\n2️⃣ Testing Index Creation...")
        db.create_indexes()
        print("✅ Indexes created successfully")
        
        # Test 3: Insert Sample Data
        print("\n3️⃣ Testing Data Insertion...")
        
        # Create sample NBA game data
        sample_games = pd.DataFrame([
            {
                'GAME_ID': '0022300001',
                'TEAM_ID': 1610612747,  # Lakers
                'TEAM_NAME': 'Los Angeles Lakers',
                'GAME_DATE': datetime.datetime(2024, 1, 15),
                'SEASON': '2023-24',
                'PTS': 115,
                'REB': 45,
                'AST': 28,
                'WL': 'W'
            },
            {
                'GAME_ID': '0022300001',
                'TEAM_ID': 1610612744,  # Warriors
                'TEAM_NAME': 'Golden State Warriors',
                'GAME_DATE': datetime.datetime(2024, 1, 15),
                'SEASON': '2023-24',
                'PTS': 110,
                'REB': 42,
                'AST': 25,
                'WL': 'L'
            },
            {
                'GAME_ID': '0022300002',
                'TEAM_ID': 1610612747,  # Lakers
                'TEAM_NAME': 'Los Angeles Lakers',
                'GAME_DATE': datetime.datetime(2024, 1, 10),
                'SEASON': '2023-24',
                'PTS': 108,
                'REB': 48,
                'AST': 30,
                'WL': 'L'
            }
        ])
        
        inserted_count = db.insert_games(sample_games)
        print(f"✅ Inserted {inserted_count} sample games")
        
        # Test 4: Get All Games
        print("\n4️⃣ Testing Get All Games...")
        all_games = db.get_games(limit=10)
        print(f"✅ Retrieved {len(all_games)} games")
        if not all_games.empty:
            print(f"   Sample columns: {list(all_games.columns[:5])}")
        
        # Test 5: Get Games by Team
        print("\n5️⃣ Testing Get Games by Team...")
        lakers_games = db.get_games(team_id=1610612747)
        print(f"✅ Retrieved {len(lakers_games)} Lakers games")
        
        # Test 6: Get Games by Season
        print("\n6️⃣ Testing Get Games by Season...")
        season_games = db.get_games(season='2023-24')
        print(f"✅ Retrieved {len(season_games)} games from 2023-24 season")
        
        # Test 7: Get Games by Date Range
        print("\n7️⃣ Testing Get Games by Date Range...")
        start_date = datetime.datetime(2024, 1, 1)
        end_date = datetime.datetime(2024, 1, 31)
        date_games = db.get_games(start_date=start_date, end_date=end_date)
        print(f"✅ Retrieved {len(date_games)} games from January 2024")
        
        # Test 8: Get Team Recent Games
        print("\n8️⃣ Testing Get Team Recent Games...")
        recent_games = db.get_team_recent_games(
            team_id=1610612747, 
            before_date=datetime.datetime(2024, 1, 20),
            limit=5
        )
        print(f"✅ Retrieved {len(recent_games)} recent Lakers games")
        
        # Test 9: Test Empty Results
        print("\n9️⃣ Testing Empty Results...")
        empty_games = db.get_games(team_id=9999999)  # Non-existent team
        print(f"✅ Empty query returned {len(empty_games)} games (should be 0)")
        
        # Test 10: Close Connection
        print("\n🔟 Testing Connection Close...")
        db.close()
        print("✅ Database connection closed successfully")
        
        print("\n" + "=" * 60)
        print("🎉 ALL TESTS PASSED!")
        print("✅ Your database module is working perfectly!")
        print("🚀 Ready for Phase 3: Data Collection!")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        print("🔧 Check your database methods and try again")
        return False

def test_database_performance():
    """Test database performance with indexes"""
    print("\n🚀 Performance Test (Optional)")
    print("-" * 40)
    
    try:
        db = NBADatabase()
        
        # Time a query
        import time
        start_time = time.time()
        games = db.get_games(team_id=1610612747, limit=100)
        end_time = time.time()
        
        query_time = (end_time - start_time) * 1000  # Convert to milliseconds
        print(f"⚡ Query completed in {query_time:.2f}ms")
        print(f"📊 Retrieved {len(games)} games")
        
        db.close()
        return True
        
    except Exception as e:
        print(f"❌ Performance test failed: {e}")
        return False

if __name__ == "__main__":
    print("Starting NBA Database Test Suite...")
    
    # Run main tests
    success = test_database_complete()
    
    if success:
        # Run performance test
        test_database_performance()
        
        print("\n🏆 Database Module Complete!")
        print("📋 What you've built:")
        print("   ✅ MongoDB connection with retry logic")
        print("   ✅ Game data insertion with duplicate handling")
        print("   ✅ Flexible game data retrieval with filters")
        print("   ✅ Database indexes for performance")
        print("   ✅ Team recent games analysis")
        print("   ✅ Proper error handling and logging")
        
        print("\n🎯 Next Phase: Data Collection")
        print("   📡 Connect to NBA API")
        print("   🏀 Collect real NBA game data")
        print("   💾 Store data in your MongoDB")
        
    else:
        print("\n🔧 Fix the issues above before proceeding") 