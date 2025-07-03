# NBA Positional Defense Integration Guide

## Overview
Positional defense data from [Hashtag Basketball](https://hashtagbasketball.com/nba-defense-vs-position) shows how each team defends against specific positions (PG, SG, SF, PF, C). This is crucial for matchup analysis and identifying exploitable weaknesses.

## Data Source
- **URL**: https://hashtagbasketball.com/nba-defense-vs-position
- **Format**: Team defensive stats allowed by position
- **Columns**: Position | Team | PTS | FG% | FT% | 3PM | REB | AST | STL | BLK | TO
- **Coverage**: All 30 NBA teams × 5 positions = 150 records

## Key Metrics Explained

### Defensive Stats Allowed
- **PTS**: Points allowed to players at this position
- **FG%**: Field goal percentage allowed to this position
- **FT%**: Free throw percentage allowed to this position
- **3PM**: Three-pointers made allowed per game
- **REB**: Rebounds allowed to this position
- **AST**: Assists allowed to this position
- **STL**: Steals allowed (lower = better defense)
- **BLK**: Blocks allowed (lower = better defense)
- **TO**: Turnovers forced from this position (higher = better defense)

### Interpretation
- **Higher values = Worse defense** (except TO - turnovers forced)
- **Lower values = Better defense** (except TO)

## Database Schema

```python
positional_defense_stats = {
    'position': str,          # 'PG', 'SG', 'SF', 'PF', 'C'
    'team_abbrev': str,       # 'LAL', 'GSW', 'BOS', etc.
    'team_info': str,         # Full team info from source
    
    # Defensive stats allowed to this position
    'pts_allowed': float,     # Points allowed per game
    'fg_pct_allowed': float,  # FG% allowed
    'ft_pct_allowed': float,  # FT% allowed
    'threes_allowed': float,  # 3PM allowed per game
    'reb_allowed': float,     # Rebounds allowed
    'ast_allowed': float,     # Assists allowed
    'stl_allowed': float,     # Steals allowed
    'blk_allowed': float,     # Blocks allowed
    'to_forced': float,       # Turnovers forced (higher = better)
    
    # Metadata
    'source': str,            # 'hashtag_basketball'
    'collected_at': datetime
}
```

## Prediction Applications

### 1. Positional Matchup Analysis

```python
def analyze_positional_matchup(team_a, team_b, position):
    """
    Analyze how Team A's position player will perform vs Team B's defense
    
    Args:
        team_a: Offensive team
        team_b: Defensive team  
        position: 'PG', 'SG', 'SF', 'PF', 'C'
    """
    
    # Get Team B's defense against this position
    defense_data = db.positional_defense_stats.find_one({
        'team_abbrev': team_b,
        'position': position
    })
    
    if not defense_data:
        return None
    
    # Analyze defensive weakness
    analysis = {
        'position': position,
        'defending_team': team_b,
        'pts_allowed': defense_data['pts_allowed'],
        'fg_pct_allowed': defense_data['fg_pct_allowed'],
        'weakness_level': get_weakness_level(defense_data['pts_allowed'], position)
    }
    
    return analysis

def get_weakness_level(pts_allowed, position):
    """Determine if this is a defensive weakness"""
    # These thresholds would be based on league averages
    thresholds = {
        'PG': {'weak': 24.0, 'average': 22.0},
        'SG': {'weak': 25.0, 'average': 23.0},
        'SF': {'weak': 24.0, 'average': 22.0},
        'PF': {'weak': 25.0, 'average': 23.0},
        'C': {'weak': 23.0, 'average': 21.0}
    }
    
    if pts_allowed > thresholds[position]['weak']:
        return 'WEAK'
    elif pts_allowed > thresholds[position]['average']:
        return 'BELOW_AVERAGE'
    else:
        return 'STRONG'
```

### 2. Team Vulnerability Matrix

```python
def create_vulnerability_matrix(team_abbrev):
    """Create a matrix showing team's defensive vulnerabilities by position"""
    
    vulnerabilities = {}
    
    for position in ['PG', 'SG', 'SF', 'PF', 'C']:
        defense_data = db.positional_defense_stats.find_one({
            'team_abbrev': team_abbrev,
            'position': position
        })
        
        if defense_data:
            vulnerabilities[position] = {
                'pts_allowed': defense_data['pts_allowed'],
                'fg_pct_allowed': defense_data['fg_pct_allowed'],
                'weakness_score': calculate_weakness_score(defense_data)
            }
    
    return vulnerabilities

def calculate_weakness_score(defense_data):
    """Calculate overall weakness score (0-100, higher = more vulnerable)"""
    
    # Normalize each metric (higher = worse defense)
    pts_score = min(defense_data['pts_allowed'] * 4, 100)  # Scale points
    fg_score = defense_data['fg_pct_allowed'] * 2  # Scale FG%
    three_score = defense_data['threes_allowed'] * 25  # Scale 3PM
    
    # Defensive positives (lower scores for good defense)
    to_score = max(0, 100 - defense_data['to_forced'] * 30)  # More TO = better defense
    
    # Weighted average
    weakness_score = (pts_score * 0.4 + fg_score * 0.3 + 
                     three_score * 0.2 + to_score * 0.1)
    
    return min(weakness_score, 100)
```

### 3. Game Prediction Features

```python
def get_positional_features(team_a, team_b):
    """Get positional matchup features for ML prediction model"""
    
    features = []
    
    for position in ['PG', 'SG', 'SF', 'PF', 'C']:
        # Team A offense vs Team B defense at this position
        team_b_defense = db.positional_defense_stats.find_one({
            'team_abbrev': team_b,
            'position': position
        })
        
        # Team B offense vs Team A defense at this position  
        team_a_defense = db.positional_defense_stats.find_one({
            'team_abbrev': team_a,
            'position': position
        })
        
        if team_a_defense and team_b_defense:
            # Positional advantage features
            features.extend([
                team_b_defense['pts_allowed'],      # How many pts Team A's position will score
                team_a_defense['pts_allowed'],      # How many pts Team B's position will score
                team_b_defense['fg_pct_allowed'],   # Team A's shooting efficiency
                team_a_defense['fg_pct_allowed'],   # Team B's shooting efficiency
                team_b_defense['threes_allowed'],   # Team A's 3pt opportunities
                team_a_defense['threes_allowed']    # Team B's 3pt opportunities
            ])
    
    return features
```

### 4. Matchup Exploitation Strategy

```python
def find_exploitable_matchups(team_a, team_b):
    """Find which positions Team A should target vs Team B"""
    
    exploitable_positions = []
    
    for position in ['PG', 'SG', 'SF', 'PF', 'C']:
        defense_data = db.positional_defense_stats.find_one({
            'team_abbrev': team_b,
            'position': position
        })
        
        if defense_data:
            # Check if this is a defensive weakness
            weakness_score = calculate_weakness_score(defense_data)
            
            if weakness_score > 70:  # Threshold for exploitable weakness
                exploitable_positions.append({
                    'position': position,
                    'weakness_score': weakness_score,
                    'pts_allowed': defense_data['pts_allowed'],
                    'fg_pct_allowed': defense_data['fg_pct_allowed'],
                    'recommendation': f"Target {position} - allows {defense_data['pts_allowed']:.1f} pts at {defense_data['fg_pct_allowed']:.1f}% FG"
                })
    
    # Sort by weakness score (most exploitable first)
    exploitable_positions.sort(key=lambda x: x['weakness_score'], reverse=True)
    
    return exploitable_positions
```

## Integration Examples

### Example 1: Lakers vs Warriors Analysis
```python
# Find Warriors' defensive weaknesses
warriors_weaknesses = find_exploitable_matchups('LAL', 'GSW')
print("Lakers should target:")
for weakness in warriors_weaknesses[:2]:  # Top 2 weaknesses
    print(f"- {weakness['recommendation']}")

# Find Lakers' defensive weaknesses  
lakers_weaknesses = find_exploitable_matchups('GSW', 'LAL')
print("Warriors should target:")
for weakness in lakers_weaknesses[:2]:
    print(f"- {weakness['recommendation']}")
```

### Example 2: Position-Specific Player Props
```python
def predict_player_performance(player_position, player_team, opponent_team):
    """Predict how a player will perform based on opponent's positional defense"""
    
    opponent_defense = db.positional_defense_stats.find_one({
        'team_abbrev': opponent_team,
        'position': player_position
    })
    
    if opponent_defense:
        expected_performance = {
            'pts_boost': max(0, opponent_defense['pts_allowed'] - 22.0),  # vs league average
            'fg_pct_boost': max(0, opponent_defense['fg_pct_allowed'] - 45.0),
            'three_pt_opportunities': opponent_defense['threes_allowed'],
            'difficulty_rating': 100 - calculate_weakness_score(opponent_defense)
        }
        
        return expected_performance
    
    return None
```

## Current Implementation Status

**Framework Built**: Positional defense collection implemented in advanced API client
**Database Schema**: Ready to store positional defense data  
**Integration Points**: Added to Phase 6 of hybrid data collector
**Analysis Functions**: Complete framework for matchup analysis
 **Data Collection**: Needs testing and refinement for HTML parsing

## Usage in Prediction Model

### Feature Importance
1. **High Impact**: Points allowed by position (direct scoring prediction)
2. **Medium Impact**: FG% allowed (efficiency prediction)
3. **Medium Impact**: 3PM allowed (three-point game impact)
4. **Low Impact**: Rebounds, assists allowed (secondary effects)

### Model Integration
```python
# Add to your ML feature pipeline
positional_features = get_positional_features(team_a, team_b)
all_features = base_features + advanced_features + positional_features

# Weight positional features based on game context
if is_playoff_game:
    positional_weight = 0.25  # Higher weight in playoffs
else:
    positional_weight = 0.15  # Standard weight

model_prediction = model.predict(all_features, feature_weights={'positional': positional_weight})
```