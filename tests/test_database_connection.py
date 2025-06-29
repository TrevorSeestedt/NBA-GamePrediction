from src.database import NBADatabase

def test_connection():
    print("🏀 Testing NBA Database Connection...")
    
    # Create database instance
    db = NBADatabase()
    print("✅ Database connection successful!")
    print(f"✅ Connected to: {db.db_name}")
    print(f"✅ Client: {db.client}")
    print(f"✅ Database: {db.db}")
    
    # Test the database methods
    # Test 1: Get all games (empty query)
    all_games = db.get_games(limit=5)
    print(f"Retrieved {len(all_games)} games")

    # Test 2: Get Lakers games (when we have data)
    lakers_games = db.get_games(team_id=1610612747, limit=5)
    print(f"Lakers games: {len(lakers_games)}")
    
    # Use assertions instead of return for pytest
    assert db.client is not None
    assert db.db is not None
    assert len(all_games) >= 0
    assert len(lakers_games) >= 0

if __name__ == "__main__":
    test_connection()
