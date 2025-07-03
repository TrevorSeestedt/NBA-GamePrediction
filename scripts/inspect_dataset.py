#!/usr/bin/env python3
"""
🔍 NBA Dataset Inspector
Explore and analyze the collected NBA dataset

Usage:
    python inspect_dataset.py
    python inspect_dataset.py --season 2023-24
    python inspect_dataset.py --collection games
"""

import argparse
import sys
import os
from datetime import datetime
import pandas as pd

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.database import NBADatabase

class DatasetInspector:
    """Inspect and analyze collected NBA data"""
    
    def __init__(self):
        self.db = NBADatabase()
        print("🔍 NBA Dataset Inspector")
        print("=" * 50)
    
    def show_database_overview(self):
        """Show overview of all collections"""
        print("📊 DATABASE OVERVIEW")
        print("-" * 30)
        
        collections = self.db.db.list_collection_names()
        total_records = 0
        
        print(f"📁 Total Collections: {len(collections)}")
        print()
        
        for collection in sorted(collections):
            count = self.db.db[collection].count_documents({})
            total_records += count
            print(f"📋 {collection:<25} {count:>8} records")
        
        print("-" * 40)
        print(f"📊 Total Records: {total_records}")
        print()
    
    def inspect_collection(self, collection_name, limit=5):
        """Inspect a specific collection"""
        print(f"🔍 INSPECTING: {collection_name}")
        print("-" * 50)
        
        collection = self.db.db[collection_name]
        
        # Count total documents
        total_count = collection.count_documents({})
        print(f"📊 Total Records: {total_count}")
        
        if total_count == 0:
            print("⚠️ Collection is empty")
            return
        
        # Show sample documents
        print(f"\n📋 Sample Records (showing {min(limit, total_count)}):")
        print("=" * 60)
        
        sample_docs = list(collection.find().limit(limit))
        
        for i, doc in enumerate(sample_docs, 1):
            print(f"\n📄 Record {i}:")
            # Show key fields in a readable format
            self._print_document_summary(doc)
        
        # Show field analysis
        print(f"\n🔧 FIELD ANALYSIS")
        print("-" * 30)
        
        if sample_docs:
            first_doc = sample_docs[0]
            print(f"📝 Total Fields: {len(first_doc)}")
            print("📋 Available Fields:")
            
            for field in sorted(first_doc.keys()):
                field_type = type(first_doc[field]).__name__
                print(f"  • {field:<25} ({field_type})")
    
    def _print_document_summary(self, doc):
        """Print a summary of a document"""
        # Priority fields to show
        priority_fields = [
            'team_name', 'player_name', 'game_id', 'game_date', 
            'season', 'wins', 'losses', 'pts', 'availability_rate',
            'days_rest', 'is_back_to_back', 'fatigue_index',
            'off_rating', 'def_rating', 'net_rating'
        ]
        
        # Show priority fields first
        shown_fields = set()
        for field in priority_fields:
            if field in doc:
                value = doc[field]
                if isinstance(value, float):
                    print(f"  {field}: {value:.3f}")
                elif isinstance(value, datetime):
                    print(f"  {field}: {value.strftime('%Y-%m-%d')}")
                else:
                    print(f"  {field}: {value}")
                shown_fields.add(field)
        
        # Show a few other interesting fields
        other_fields = [k for k in doc.keys() if k not in shown_fields and not k.startswith('_')]
        if other_fields:
            print(f"  ... and {len(other_fields)} other fields")
    
    def analyze_injury_data(self, season=None):
        """Special analysis for injury/availability data"""
        print("🏥 INJURY/AVAILABILITY ANALYSIS")
        print("=" * 40)
        
        query = {}
        if season:
            query['season'] = season
        
        injury_docs = list(self.db.db.player_injury_data.find(query))
        
        if not injury_docs:
            print("⚠️ No injury data found")
            return
        
        print(f"📊 Players Analyzed: {len(injury_docs)}")
        
        # Availability rate analysis
        availability_rates = [doc['availability_rate'] for doc in injury_docs]
        avg_availability = sum(availability_rates) / len(availability_rates)
        
        print(f"📈 Average Availability Rate: {avg_availability:.1%}")
        
        # Find players with concerning availability
        concerning_players = [
            doc for doc in injury_docs 
            if doc['availability_rate'] < 0.8
        ]
        
        print(f"⚠️ Players with <80% Availability: {len(concerning_players)}")
        
        if concerning_players:
            print("\n🚨 Most Concerning Cases:")
            sorted_players = sorted(concerning_players, key=lambda x: x['availability_rate'])
            
            for player in sorted_players[:5]:
                print(f"  • {player['player_name']}: {player['availability_rate']:.1%} available")
                print(f"    Status: {player['injury_status']}")
                print(f"    Games missed: {player['games_missed_estimated']}")
                print()
    
    def analyze_rest_fatigue(self, season=None):
        """Special analysis for rest/fatigue data"""
        print("😴 REST/FATIGUE ANALYSIS")
        print("=" * 40)
        
        query = {}
        if season:
            query['season'] = season
        
        rest_docs = list(self.db.db.team_rest_fatigue.find(query))
        
        if not rest_docs:
            print("⚠️ No rest/fatigue data found")
            return
        
        print(f"📊 Game Records: {len(rest_docs)}")
        
        # Back-to-back analysis
        b2b_games = [doc for doc in rest_docs if doc['is_back_to_back']]
        print(f"🔄 Back-to-Back Games: {len(b2b_games)} ({len(b2b_games)/len(rest_docs)*100:.1f}%)")
        
        # Fatigue analysis
        fatigue_scores = [doc['fatigue_index'] for doc in rest_docs]
        avg_fatigue = sum(fatigue_scores) / len(fatigue_scores)
        print(f"😴 Average Fatigue Index: {avg_fatigue:.3f}")
        
        # High fatigue situations
        high_fatigue = [doc for doc in rest_docs if doc['fatigue_index'] > 0.7]
        print(f"🥵 High Fatigue Games: {len(high_fatigue)} ({len(high_fatigue)/len(rest_docs)*100:.1f}%)")
        
        # Rest distribution
        rest_distribution = {}
        for doc in rest_docs:
            days = doc['days_rest']
            rest_distribution[days] = rest_distribution.get(days, 0) + 1
        
        print("\n📊 Rest Days Distribution:")
        for days in sorted(rest_distribution.keys())[:7]:  # Show first 7 days
            count = rest_distribution[days]
            percentage = count / len(rest_docs) * 100
            print(f"  {days} days rest: {count} games ({percentage:.1f}%)")
    
    def analyze_team_performance(self, season=None):
        """Analyze team performance data"""
        print("🏀 TEAM PERFORMANCE ANALYSIS")
        print("=" * 40)
        
        query = {}
        if season:
            query['season'] = season
        
        # Advanced stats analysis
        advanced_docs = list(self.db.db.team_advanced_stats.find(query))
        
        if advanced_docs:
            print(f"📊 Teams with Advanced Stats: {len(advanced_docs)}")
            
            # Find best offensive and defensive teams
            best_offense = max(advanced_docs, key=lambda x: x['off_rating'])
            best_defense = min(advanced_docs, key=lambda x: x['def_rating'])
            
            print(f"\n🔥 Best Offense: {best_offense['team_name']}")
            print(f"   Offensive Rating: {best_offense['off_rating']:.1f}")
            
            print(f"\n🛡️ Best Defense: {best_defense['team_name']}")
            print(f"   Defensive Rating: {best_defense['def_rating']:.1f}")
        
        # Chemistry analysis
        chemistry_docs = list(self.db.db.team_chemistry_stats.find(query))
        
        if chemistry_docs:
            print(f"\n🧪 Teams with Chemistry Data: {len(chemistry_docs)}")
            
            # Find teams with best chemistry
            teams_with_index = [doc for doc in chemistry_docs if 'chemistry_index' in doc]
            if teams_with_index:
                best_chemistry = max(teams_with_index, key=lambda x: x['chemistry_index'])
                print(f"\n🤝 Best Team Chemistry: {best_chemistry['team_name']}")
                print(f"   Chemistry Index: {best_chemistry['chemistry_index']:.1f}")

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Inspect NBA dataset')
    parser.add_argument('--season', help='Filter by season (e.g., 2023-24)')
    parser.add_argument('--collection', help='Inspect specific collection')
    parser.add_argument('--analysis', choices=['injury', 'rest', 'team'], 
                       help='Run specific analysis')
    
    args = parser.parse_args()
    
    inspector = DatasetInspector()
    
    try:
        if args.collection:
            # Inspect specific collection
            inspector.inspect_collection(args.collection)
        elif args.analysis:
            # Run specific analysis
            if args.analysis == 'injury':
                inspector.analyze_injury_data(args.season)
            elif args.analysis == 'rest':
                inspector.analyze_rest_fatigue(args.season)
            elif args.analysis == 'team':
                inspector.analyze_team_performance(args.season)
        else:
            # Show overview and run all analyses
            inspector.show_database_overview()
            
            print("\n" + "="*60)
            inspector.analyze_injury_data(args.season)
            
            print("\n" + "="*60)
            inspector.analyze_rest_fatigue(args.season)
            
            print("\n" + "="*60)
            inspector.analyze_team_performance(args.season)
    
    except Exception as e:
        print(f"❌ Error during inspection: {e}")

if __name__ == "__main__":
    main() 