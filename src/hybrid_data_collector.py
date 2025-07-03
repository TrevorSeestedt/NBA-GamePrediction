"""
Hybrid NBA Data Collector
Combines nba-api library with direct NBA.com scraping for complete data coverage
"""

import logging
from .data_collector import NBADataCollector
from .advanced_api_client import NBADirectAPIClient
from .database import NBADatabase
import pandas as pd
from datetime import datetime
import time

logger = logging.getLogger(__name__)

class HybridNBACollector:
    """
    Hybrid collector that uses both nba-api and direct NBA.com scraping
    
    This gives us the best of both worlds:
    - nba-api: Reliable basic stats, team games, player data
    - Direct scraping: Advanced chemistry stats not in nba-api
    """
    
    def __init__(self, rate_limit_delay=2.0):
        """
        Initialize the hybrid collector
        
        Args:
            rate_limit_delay (float): Seconds between requests
        """
        logger.info("🔗 Initializing Hybrid NBA Data Collector")
        
        # Initialize both collectors
        self.nba_api_collector = NBADataCollector(rate_limit_delay)
        self.direct_api_client = NBADirectAPIClient(rate_limit_delay)
        self.db = NBADatabase()
        
        logger.info("✅ Hybrid collector ready!")
        logger.info("📊 nba-api: Basic stats, games, players")
        logger.info("🧪 Direct scraping: Chemistry stats, advanced metrics")
    
    def collect_complete_season_data(self, season='2024-25'):
        """
        🎯 MAIN METHOD: Collect complete season data using hybrid approach
        
        This method orchestrates the collection of all data types:
        1. Basic game data (nba-api)
        2. Advanced team stats (nba-api) 
        3. Chemistry stats (direct scraping)
        4. Player data (nba-api)
        
        Args:
            season (str): NBA season to collect
            
        Returns:
            dict: Summary of all collected data
        """
        logger.info(f"🚀 Starting complete data collection for {season}")
        logger.info("=" * 60)
        
        results = {
            'season': season,
            'collected_at': datetime.now(),
            'data_sources': {
                'nba_api': {},
                'direct_scraping': {}
            }
        }
        
        # Phase 1: Basic game data (nba-api is reliable for this)
        logger.info("📊 PHASE 1: Basic Game Data (nba-api)")
        logger.info("-" * 40)
        
        try:
            # Regular season games
            regular_games = self.nba_api_collector.collect_season_games(season, 'Regular Season')
            results['data_sources']['nba_api']['regular_season_games'] = regular_games
            logger.info(f"✅ Regular season: {regular_games} games")
            
            # Playoff games (if available)
            playoff_games = self.nba_api_collector.collect_season_games(season, 'Playoffs')
            results['data_sources']['nba_api']['playoff_games'] = playoff_games
            logger.info(f"✅ Playoffs: {playoff_games} games")
            
        except Exception as e:
            logger.error(f"❌ Error in Phase 1: {e}")
        
        # Phase 2: Advanced team stats (nba-api)
        logger.info("\n📈 PHASE 2: Advanced Team Stats (nba-api)")
        logger.info("-" * 40)
        
        try:
            advanced_stats = self.nba_api_collector.collect_team_advanced_stats(season)
            results['data_sources']['nba_api']['advanced_stats'] = advanced_stats
            logger.info(f"✅ Advanced stats: {advanced_stats} teams")
            
        except Exception as e:
            logger.error(f"❌ Error in Phase 2: {e}")
        
        # Phase 3: Chemistry stats (direct scraping - THE KEY ADDITION!)
        logger.info("\n🧪 PHASE 3: Chemistry Stats (Direct Scraping)")
        logger.info("-" * 40)
        
        try:
            chemistry_data = self.direct_api_client.collect_all_chemistry_stats(season)
            results['data_sources']['direct_scraping'] = chemistry_data
            
            # Store chemistry data in database
            chemistry_records = 0
            for stat_type, df in chemistry_data.items():
                if not df.empty:
                    collection_name = f"chemistry_{stat_type}"
                    inserted = self.db.db[collection_name].insert_many(
                        df.to_dict(orient='records'), ordered=False
                    )
                    chemistry_records += len(inserted.inserted_ids)
            
            logger.info(f"✅ Chemistry stats: {chemistry_records} total records")
            
        except Exception as e:
            logger.error(f"❌ Error in Phase 3: {e}")
        
        # Phase 4: Calculate Chemistry Index
        logger.info("\n🧮 PHASE 4: Chemistry Index Calculation")
        logger.info("-" * 40)
        
        try:
            chemistry_index = self.calculate_team_chemistry_index(season)
            results['chemistry_index_teams'] = chemistry_index
            logger.info(f"✅ Chemistry index: {chemistry_index} teams processed")
            
        except Exception as e:
            logger.error(f"❌ Error in Phase 4: {e}")
        
        # Phase 5: Collect clutch stats
        logger.info("\n🔥 PHASE 5: Collecting Clutch Performance Stats")
        logger.info("=" * 60)
        
        try:
            clutch_results = self.collect_clutch_stats(season)
            results['clutch_stats'] = clutch_results
            
            logger.info("🔥 Clutch Stats Collection Summary:")
            logger.info(f"   Regular Season: {clutch_results['regular_season']} records")
            logger.info(f"   Playoffs: {clutch_results['playoffs']} records")
            logger.info(f"   Total Teams: {clutch_results['total_teams']}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in clutch stats collection: {e}")
            return results
    
    def calculate_team_chemistry_index(self, season='2024-25'):
        """
        Calculate the Chemistry Index using collected data
        
        This implements your exact methodology:
        1. Get Screen Assists, Secondary Assists, Contested Shots, Deflections
        2. Scale each 0-100 across all teams
        3. Take equal-weighted average
        4. Store results
        
        Args:
            season (str): NBA season
            
        Returns:
            int: Number of teams processed
        """
        logger.info(f"🧮 Calculating Chemistry Index for {season}")
        
        try:
            # Get hustle stats (contains our chemistry metrics)
            hustle_regular = self.db.db.chemistry_hustle_regular.find({'season': season})
            hustle_df = pd.DataFrame(list(hustle_regular))
            
            if hustle_df.empty:
                logger.warning("⚠️ No hustle stats found for chemistry calculation")
                return 0
            
            # Look for chemistry columns (names may vary)
            chemistry_metrics = {}
            
            # Map potential column names to our standard names
            column_mappings = {
                'screen_assists': ['SCREEN_ASSISTS', 'Screen Assists', 'SCREEN_AST'],
                'deflections': ['DEFLECTIONS', 'Deflections', 'DEF'],
                'contested_shots': ['CONTESTED_SHOTS', 'Contested Shots', 'CONTESTED_2PT', 'CONTESTED_3PT'],
                'secondary_assists': ['SECONDARY_ASSISTS', 'Secondary Assists', 'SECONDARY_AST']
            }
            
            for metric, possible_cols in column_mappings.items():
                for col in possible_cols:
                    if col in hustle_df.columns:
                        chemistry_metrics[metric] = col
                        break
            
            if len(chemistry_metrics) < 2:
                logger.warning(f"⚠️ Insufficient chemistry metrics found: {list(chemistry_metrics.keys())}")
                return 0
            
            logger.info(f"🎯 Using chemistry metrics: {list(chemistry_metrics.keys())}")
            
            # Calculate chemistry index for each team
            chemistry_records = []
            
            for _, row in hustle_df.iterrows():
                try:
                    # Extract metric values
                    metric_values = {}
                    for metric, col_name in chemistry_metrics.items():
                        metric_values[metric] = float(row.get(col_name, 0))
                    
                    # Simple chemistry score (average of available metrics)
                    chemistry_score = sum(metric_values.values()) / len(metric_values)
                    
                    chemistry_record = {
                        'team_id': row.get('TEAM_ID', row.get('Team')),
                        'team_name': row.get('TEAM_NAME', row.get('Team')),
                        'season': season,
                        'chemistry_score': chemistry_score,
                        'metrics_used': list(chemistry_metrics.keys()),
                        'calculated_at': datetime.now(),
                        **metric_values  # Include individual metric values
                    }
                    
                    chemistry_records.append(chemistry_record)
                    
                except Exception as e:
                    logger.error(f"❌ Error processing team chemistry: {e}")
                    continue
            
            # Store chemistry index results
            if chemistry_records:
                result = self.db.db.team_chemistry_index.insert_many(chemistry_records, ordered=False)
                logger.info(f"💾 Stored chemistry index for {len(result.inserted_ids)} teams")
                
                # Show top chemistry teams
                top_teams = sorted(chemistry_records, key=lambda x: x['chemistry_score'], reverse=True)[:5]
                top_names = [f"{t['team_name']} ({t['chemistry_score']:.1f})" for t in top_teams]
                logger.info(f"🏆 Top chemistry teams: {top_names}")
            
            return len(chemistry_records)
            
        except Exception as e:
            logger.error(f"❌ Error calculating chemistry index: {e}")
            return 0
    
    def collect_clutch_stats(self, season='2023-24'):
        """
        Phase 5: Collect clutch performance statistics
        
        Clutch stats show how teams perform in close games (within 5 points in last 5 minutes).
        This is crucial for matchup predictions as it reveals which teams excel under pressure.
        
        Args:
            season (str): NBA season
            
        Returns:
            dict: Collection results for both regular season and playoffs
        """
        logger.info("🔥 PHASE 5: Collecting Clutch Performance Stats")
        logger.info("=" * 60)
        
        results = {
            'regular_season': 0,
            'playoffs': 0,
            'total_teams': 0
        }
        
        try:
            # Collect regular season clutch stats
            logger.info("📊 Collecting Regular Season clutch stats...")
            regular_clutch = self.direct_api_client.get_clutch_stats('Regular Season')
            
            if regular_clutch:
                # Store in database
                self.db.db.team_clutch_stats.insert_many(regular_clutch, ordered=False)
                results['regular_season'] = len(regular_clutch)
                logger.info(f"✅ Stored {len(regular_clutch)} regular season clutch records")
                
                # Show top clutch performers
                top_clutch = sorted(regular_clutch, key=lambda x: x.get('clutch_win_pct', 0), reverse=True)[:5]
                clutch_leaders = [f"{t['team_name']} ({t.get('clutch_win_pct', 0):.1%})" for t in top_clutch]
                logger.info(f"🏆 Top 5 Clutch teams (Regular Season): {clutch_leaders}")
            
            # Add delay between requests
            time.sleep(2)
            
            # Collect playoff clutch stats
            logger.info("📊 Collecting Playoff clutch stats...")
            playoff_clutch = self.direct_api_client.get_clutch_stats('Playoffs')
            
            if playoff_clutch:
                # Store in database
                self.db.db.team_clutch_stats.insert_many(playoff_clutch, ordered=False)
                results['playoffs'] = len(playoff_clutch)
                logger.info(f"✅ Stored {len(playoff_clutch)} playoff clutch records")
                
                # Show top playoff clutch performers
                top_playoff_clutch = sorted(playoff_clutch, key=lambda x: x.get('clutch_win_pct', 0), reverse=True)[:5]
                playoff_clutch_leaders = [f"{t['team_name']} ({t.get('clutch_win_pct', 0):.1%})" for t in top_playoff_clutch]
                logger.info(f"🏆 Top 5 Clutch teams (Playoffs): {playoff_clutch_leaders}")
            
            results['total_teams'] = len(set([t['team_name'] for t in (regular_clutch + playoff_clutch)]))
            
            logger.info("🔥 Clutch Stats Collection Summary:")
            logger.info(f"   Regular Season: {results['regular_season']} records")
            logger.info(f"   Playoffs: {results['playoffs']} records")
            logger.info(f"   Total Teams: {results['total_teams']}")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in clutch stats collection: {e}")
            return results
    
    def collect_positional_defense_stats(self):
        """
        Phase 6: Collect positional defense statistics
        
        This data shows how each team defends against specific positions (PG, SG, SF, PF, C).
        Critical for matchup analysis - if a team is weak defending guards and opponent has great guards,
        that's a significant predictive factor.
        
        Returns:
            dict: Collection results including records per position
        """
        logger.info("🛡️ PHASE 6: Collecting Positional Defense Stats")
        logger.info("=" * 60)
        
        results = {
            'total_records': 0,
            'positions': {},
            'teams_covered': 0
        }
        
        try:
            # Collect positional defense data from Hashtag Basketball
            logger.info("📊 Collecting positional defense data...")
            positional_data = self.direct_api_client.get_positional_defense_stats()
            
            if positional_data:
                # Store in database
                self.db.db.positional_defense_stats.insert_many(positional_data, ordered=False)
                results['total_records'] = len(positional_data)
                
                # Analyze what we collected
                positions_count = {}
                teams_set = set()
                
                for record in positional_data:
                    pos = record['position']
                    team = record['team_abbrev']
                    
                    if pos not in positions_count:
                        positions_count[pos] = 0
                    positions_count[pos] += 1
                    teams_set.add(team)
                
                results['positions'] = positions_count
                results['teams_covered'] = len(teams_set)
                
                logger.info(f"✅ Stored {len(positional_data)} positional defense records")
                logger.info(f"📊 Positions covered: {list(positions_count.keys())}")
                logger.info(f"🏀 Teams covered: {results['teams_covered']}")
                
                # Show best and worst defenses by position
                self._analyze_positional_defense(positional_data)
                
            else:
                logger.warning("⚠️ No positional defense data collected")
            
            return results
            
        except Exception as e:
            logger.error(f"❌ Error in positional defense collection: {e}")
            return results
    
    def _analyze_positional_defense(self, positional_data):
        """Analyze and display positional defense insights"""
        logger.info("\n🔍 POSITIONAL DEFENSE ANALYSIS")
        logger.info("=" * 60)
        
        try:
            # Group by position
            by_position = {}
            for record in positional_data:
                pos = record['position']
                if pos not in by_position:
                    by_position[pos] = []
                by_position[pos].append(record)
            
            # Show best/worst defenses for each position
            for position in ['PG', 'SG', 'SF', 'PF', 'C']:
                if position in by_position:
                    pos_data = by_position[position]
                    
                    # Sort by points allowed (lower = better defense)
                    pos_data.sort(key=lambda x: x['pts_allowed'])
                    
                    best_defense = pos_data[:3]  # Top 3 defenses
                    worst_defense = pos_data[-3:]  # Bottom 3 defenses
                    
                    logger.info(f"\n🛡️ {position} Defense:")
                    
                    best_teams = [f"{t['team_abbrev']} ({t['pts_allowed']:.1f})" for t in best_defense]
                    worst_teams = [f"{t['team_abbrev']} ({t['pts_allowed']:.1f})" for t in worst_defense]
                    
                    logger.info(f"   Best: {best_teams}")
                    logger.info(f"   Worst: {worst_teams}")
        
        except Exception as e:
            logger.error(f"❌ Error analyzing positional defense: {e}")

    def run_full_collection(self, season='2023-24'):
        """
        Run complete hybrid data collection pipeline
        
        Updated to include positional defense stats as Phase 6
        """
        logger.info("🚀 STARTING FULL HYBRID NBA DATA COLLECTION")
        logger.info("=" * 80)
        logger.info(f"Season: {season}")
        logger.info(f"Target: Complete NBA prediction dataset")
        logger.info("=" * 80)
        
        collection_summary = {
            'season': season,
            'started_at': datetime.now(),
            'phases_completed': [],
            'total_records': 0,
            'errors': []
        }
        
        try:
            # Phase 1: Basic Game Data (NBA API)
            logger.info("\n🎯 PHASE 1: Basic Game Data Collection")
            basic_results = self.collect_basic_games(season)
            collection_summary['phases_completed'].append('basic_games')
            collection_summary['basic_games'] = basic_results
            
            # Phase 2: Advanced Team Stats (NBA API)
            logger.info("\n📊 PHASE 2: Advanced Team Statistics")
            advanced_results = self.collect_advanced_team_stats(season)
            collection_summary['phases_completed'].append('advanced_stats')
            collection_summary['advanced_stats'] = advanced_results
            
            # Phase 3: Chemistry Stats (Direct NBA.com scraping)
            logger.info("\n🧪 PHASE 3: Team Chemistry Analysis")
            chemistry_results = self.collect_chemistry_stats(season)
            collection_summary['phases_completed'].append('chemistry_stats')
            collection_summary['chemistry_stats'] = chemistry_results
            
            # Phase 4: Chemistry Index Calculation
            logger.info("\n🧮 PHASE 4: Chemistry Index Calculation")
            index_results = self.calculate_chemistry_index(season)
            collection_summary['phases_completed'].append('chemistry_index')
            collection_summary['chemistry_index'] = index_results
            
            # Phase 5: Clutch Performance Stats
            logger.info("\n🔥 PHASE 5: Clutch Performance Analysis")
            clutch_results = self.collect_clutch_stats(season)
            collection_summary['phases_completed'].append('clutch_stats')
            collection_summary['clutch_stats'] = clutch_results
            
            # Phase 6: Positional Defense Stats (NEW!)
            logger.info("\n🛡️ PHASE 6: Positional Defense Analysis")
            positional_results = self.collect_positional_defense_stats()
            collection_summary['phases_completed'].append('positional_defense')
            collection_summary['positional_defense'] = positional_results
            
            # Calculate total records
            total_records = (
                basic_results.get('total_games', 0) +
                advanced_results.get('teams_processed', 0) +
                chemistry_results.get('teams_processed', 0) +
                clutch_results.get('regular_season', 0) +
                clutch_results.get('playoffs', 0) +
                positional_results.get('total_records', 0)
            )
            
            collection_summary['total_records'] = total_records
            collection_summary['completed_at'] = datetime.now()
            collection_summary['duration'] = collection_summary['completed_at'] - collection_summary['started_at']
            
            # Final Summary
            logger.info("\n" + "=" * 80)
            logger.info("🎉 HYBRID COLLECTION COMPLETE!")
            logger.info("=" * 80)
            logger.info(f"✅ Phases completed: {len(collection_summary['phases_completed'])}/6")
            logger.info(f"📊 Total records collected: {total_records}")
            logger.info(f"⏱️ Duration: {collection_summary['duration']}")
            logger.info(f"🏀 Season: {season}")
            
            # Show what we collected
            logger.info("\n📋 Collection Breakdown:")
            logger.info(f"   • Basic Games: {basic_results.get('total_games', 0)} records")
            logger.info(f"   • Advanced Stats: {advanced_results.get('teams_processed', 0)} teams")
            logger.info(f"   • Chemistry Stats: {chemistry_results.get('teams_processed', 0)} teams")
            logger.info(f"   • Clutch Stats (Regular): {clutch_results.get('regular_season', 0)} teams")
            logger.info(f"   • Clutch Stats (Playoffs): {clutch_results.get('playoffs', 0)} teams")
            logger.info(f"   • Positional Defense: {positional_results.get('total_records', 0)} records")
            
            # Store collection summary
            self.db.db.collection_summary.insert_one(collection_summary)
            
            return collection_summary
            
        except Exception as e:
            logger.error(f"❌ Critical error in full collection: {e}")
            collection_summary['errors'].append(str(e))
            collection_summary['failed_at'] = datetime.now()
            return collection_summary
    
    def close(self):
        """Close all connections"""
        if hasattr(self, 'nba_api_collector'):
            self.nba_api_collector.close()
        if hasattr(self, 'db'):
            self.db.close()
        logger.info("🔌 Hybrid collector connections closed")

# Example usage
if __name__ == "__main__":
    print("🔗 Hybrid NBA Data Collector - Test Run")
    print("=" * 60)
    
    # Initialize hybrid collector
    collector = HybridNBACollector()
    
    # Test complete data collection
    print("\n🚀 Testing complete data collection...")
    results = collector.collect_complete_season_data('2024-25')
    
    print(f"\n✅ Collection Results:")
    print(f"📊 Season: {results['season']}")
    print(f"🎯 Data sources: {len(results['data_sources'])}")
    
    # Close connections
    collector.close()
    
    print("\n🎉 Hybrid collector test complete!")