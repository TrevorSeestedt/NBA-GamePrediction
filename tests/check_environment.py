#!/usr/bin/env python3
"""
NBA Game Predictor - Environment Setup Checker
This script verifies that your environment is properly configured.
"""

import sys
import os
import subprocess
from pathlib import Path

def print_header(title):
    """Print a formatted header"""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def print_status(item, status, details=""):
    """Print status with formatting"""
    status_symbol = "✅" if status else "❌"
    print(f"{status_symbol} {item}")
    if details:
        print(f"   {details}")

def check_python_version():
    """Check Python version"""
    print_header("PYTHON VERSION CHECK")
    
    version = sys.version_info
    print(f"Python version: {version.major}.{version.minor}.{version.micro}")
    
    if version >= (3, 8):
        print_status("Python version", True, f"Version {version.major}.{version.minor} is compatible")
        return True
    else:
        print_status("Python version", False, f"Need Python 3.8+, found {version.major}.{version.minor}")
        return False

def check_virtual_environment():
    """Check if virtual environment is active"""
    print_header("VIRTUAL ENVIRONMENT CHECK")
    
    # Check if we're in a virtual environment
    in_venv = hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix)
    
    if in_venv:
        venv_path = sys.prefix
        print_status("Virtual environment", True, f"Active at: {venv_path}")
        return True
    else:
        print_status("Virtual environment", False, "No virtual environment detected")
        print("   💡 Run: python -m venv nba_env && nba_env\\Scripts\\activate (Windows)")
        print("   💡 Or: python -m venv nba_env && source nba_env/bin/activate (Mac/Linux)")
        return False

def check_required_packages():
    """Check if required packages are installed"""
    print_header("REQUIRED PACKAGES CHECK")
    
    required_packages = {
        'nba_api': 'NBA API client',
        'pandas': 'Data manipulation',
        'numpy': 'Numerical computing',
        'scikit-learn': 'Machine learning',
        'pymongo': 'MongoDB client',
        'matplotlib': 'Plotting',
        'seaborn': 'Statistical plotting',
        'requests': 'HTTP requests',
        'joblib': 'Model persistence'
    }
    
    missing_packages = []
    
    for package, description in required_packages.items():
        try:
            __import__(package)
            print_status(f"{package}", True, description)
        except ImportError:
            print_status(f"{package}", False, f"Missing - {description}")
            missing_packages.append(package)
    
    if missing_packages:
        print(f"\n💡 Install missing packages:")
        if 'nba_api' in missing_packages:
            print("   pip install nba-api")
        if 'pymongo' in missing_packages:
            print("   pip install pymongo motor dnspython")
        print("   pip install pandas numpy scikit-learn matplotlib seaborn requests joblib")
        return False
    
    return True

def check_project_structure():
    """Check project directory structure"""
    print_header("PROJECT STRUCTURE CHECK")
    
    required_dirs = ['src', 'data', 'models', 'logs', 'tests', 'notebooks']
    required_files = ['requirements.txt', '.gitignore', 'README.md']
    
    all_good = True
    
    # Check directories
    for dir_name in required_dirs:
        if os.path.exists(dir_name):
            print_status(f"Directory: {dir_name}/", True)
        else:
            print_status(f"Directory: {dir_name}/", False, "Missing directory")
            all_good = False
    
    # Check files
    for file_name in required_files:
        if os.path.exists(file_name):
            print_status(f"File: {file_name}", True)
        else:
            print_status(f"File: {file_name}", False, "Missing file")
            all_good = False
    
    # Check src files
    src_files = ['__init__.py', 'database.py', 'data_collector.py', 'data_processor.py', 'predictor.py', 'main.py']
    for file_name in src_files:
        file_path = f"src/{file_name}"
        if os.path.exists(file_path):
            # Check if file has content
            if os.path.getsize(file_path) > 0:
                print_status(f"src/{file_name}", True, "File exists with content")
            else:
                print_status(f"src/{file_name}", False, "File exists but is empty")
        else:
            print_status(f"src/{file_name}", False, "File missing")
            all_good = False
    
    return all_good

def check_nba_api_connection():
    """Test NBA API connection"""
    print_header("NBA API CONNECTION TEST")
    
    try:
        from nba_api.stats.static import teams
        nba_teams = teams.get_teams()
        
        if nba_teams and len(nba_teams) > 0:
            print_status("NBA API connection", True, f"Successfully retrieved {len(nba_teams)} teams")
            print(f"   Sample team: {nba_teams[0]['full_name']}")
            return True
        else:
            print_status("NBA API connection", False, "No teams data retrieved")
            return False
            
    except ImportError:
        print_status("NBA API connection", False, "nba_api package not installed")
        return False
    except Exception as e:
        print_status("NBA API connection", False, f"Connection error: {str(e)}")
        return False

def check_mongodb_availability():
    """Check if MongoDB is available"""
    print_header("MONGODB AVAILABILITY CHECK")
    
    try:
        import pymongo
        
        # Try to connect to MongoDB
        client = pymongo.MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        
        print_status("MongoDB connection", True, "MongoDB is running and accessible")
        return True
        
    except ImportError:
        print_status("MongoDB connection", False, "pymongo package not installed")
        return False
    except Exception as e:
        print_status("MongoDB connection", False, f"MongoDB not available: {str(e)}")
        print("   💡 Start MongoDB with: docker-compose up -d")
        print("   💡 Or install MongoDB locally")
        return False

def create_missing_directories():
    """Create missing directories"""
    print_header("CREATING MISSING DIRECTORIES")
    
    required_dirs = ['src', 'data', 'models', 'logs', 'tests', 'notebooks']
    
    for dir_name in required_dirs:
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
            print_status(f"Created: {dir_name}/", True)
        else:
            print_status(f"Exists: {dir_name}/", True)

def generate_setup_commands():
    """Generate setup commands based on what's missing"""
    print_header("SETUP COMMANDS")
    
    print("If you need to set up from scratch, run these commands:")
    print()
    print("# 1. Create and activate virtual environment")
    print("python -m venv nba_env")
    print("# Windows: nba_env\\Scripts\\activate")
    print("# Mac/Linux: source nba_env/bin/activate")
    print()
    print("# 2. Install required packages")
    print("pip install nba-api pandas numpy scikit-learn matplotlib seaborn")
    print("pip install pymongo motor dnspython requests joblib")
    print()
    print("# 3. Start MongoDB (if using Docker)")
    print("docker-compose up -d")
    print()
    print("# 4. Test the setup")
    print("python check_environment.py")

def main():
    """Main environment check function"""
    print("🏀 NBA Game Predictor - Environment Setup Checker")
    print("This will verify your development environment is ready.")
    
    checks = []
    
    # Run all checks
    checks.append(("Python Version", check_python_version()))
    checks.append(("Virtual Environment", check_virtual_environment()))
    checks.append(("Required Packages", check_required_packages()))
    checks.append(("Project Structure", check_project_structure()))
    checks.append(("NBA API Connection", check_nba_api_connection()))
    checks.append(("MongoDB Availability", check_mongodb_availability()))
    
    # Create missing directories
    create_missing_directories()
    
    # Summary
    print_header("SETUP SUMMARY")
    
    passed = sum(1 for _, status in checks if status)
    total = len(checks)
    
    for check_name, status in checks:
        print_status(check_name, status)
    
    print(f"\n📊 Overall Status: {passed}/{total} checks passed")
    
    if passed == total:
        print("\n🎉 Congratulations! Your environment is fully set up!")
        print("✨ You're ready to start building the NBA Game Predictor!")
        print("\n🚀 Next steps:")
        print("   1. Run: python src/database.py (test database)")
        print("   2. Run: python src/data_collector.py (collect NBA data)")
        print("   3. Start building your predictor!")
    else:
        print(f"\n⚠️  Setup incomplete: {total - passed} issues need attention")
        generate_setup_commands()
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 