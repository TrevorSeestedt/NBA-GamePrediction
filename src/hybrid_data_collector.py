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
        
        # Summary
        logger.info("\n🎉 COLLECTION COMPLETE!")
        logger.info("=" * 60)
        
        total_nba_api = sum(results['data_sources']['nba_api'].values())
        total_direct = sum(len(df) for df in results['data_sources']['direct_scraping'].values() if not df.empty)
        
        logger.info(f"📊 nba-api records: {total_nba_api}")
        logger.info(f"🧪 Direct scraping records: {total_direct}")
        logger.info(f"🏆 Total records: {total_nba_api + total_direct}")
        
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