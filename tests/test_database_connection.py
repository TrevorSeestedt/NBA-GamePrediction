from src.database import NBADatabase

def test_connection():
    print("🏀 Testing NBA Database Connection...")
    
    try:
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
        
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("💡 Make sure MongoDB is running: docker-compose up -d")
        return False

if __name__ == "__main__":
    test_connection()
