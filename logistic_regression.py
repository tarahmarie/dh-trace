"""
Logistic Regression Analysis for Literary Influence Detection

This script identifies literary influence by calibrating weights against a
KNOWN influence relationship, then applying those weights to discover unknown
influence candidates.

METHOD:
    1. Load three predictor variables for all text pairs:
       - Hapax Legomena Jaccard Distance (hap_jac_dis) - vocabulary similarity
       - Sequence Alignment Jaccard Distance (al_jac_dis) - phrasing similarity
       - SVM Confidence Score (svm_score) - stylistic similarity
    
    2. Find the ANCHOR CASE: Eliot ch77 -> Lawrence ch29
       A known literary influence relationship validated by scholarship.
    
    3. CALIBRATE WEIGHTS: Search for the weight combination that maximizes
       the anchor case's influence score. This tells us what TYPE of similarity
       best captures influence in this corpus (vocabulary? phrasing? style?)
    
    4. APPLY CALIBRATED WEIGHTS to all cross-author pairs to find other
       influence candidates.
    
    5. Run logistic regression for comparison with the calibrated approach.

KEY INSIGHT: We're not just doing authorship attribution. The goal is to find
pairs where different authors show unexpected similarity - these are influence
candidates. The anchor case lets us tune the weights before searching.

Output includes:
    - Calibrated weights and what they reveal about influence
    - Ranking of cross-author pairs by influence score
    - The Eliot-Lawrence anchor case position
    - Top influence candidates for literary investigation
    - Logistic regression coefficients for comparison

Author: Tarah Wheeler
For: DH-Trace Dissertation Project / UK-Ireland DH Association 2025
"""

import sqlite3
import numpy as np
import pandas as pd
from scipy import stats
import statsmodels.api as sm
from sklearn.metrics import roc_auc_score

from util import get_project_name


def load_data():
    """
    Load and prepare data from the project databases.
    Returns a DataFrame with predictor variables, target, and text identifiers.
    """
    print("=" * 70)
    print("STEP 1: LOADING DATA")
    print("=" * 70)
    
    project_name = get_project_name()
    
    # Connect to main database
    main_db_path = f"./projects/{project_name}/db/{project_name}.db"
    print(f"\nConnecting to: {main_db_path}")
    
    main_conn = sqlite3.connect(main_db_path)
    
    # Load combined_jaccard with text names for identification
    query = """
    SELECT 
        cj.source_auth,
        cj.target_auth,
        cj.source_text,
        cj.target_text,
        cj.hap_jac_dis,
        cj.al_jac_dis,
        cj.pair_id,
        a1.source_filename as source_name,
        a2.source_filename as target_name,
        auth1.author_name as source_author_name,
        auth2.author_name as target_author_name
    FROM combined_jaccard cj
    JOIN all_texts a1 ON cj.source_text = a1.text_id
    JOIN all_texts a2 ON cj.target_text = a2.text_id
    JOIN authors auth1 ON cj.source_auth = auth1.id
    JOIN authors auth2 ON cj.target_auth = auth2.id
    """
    
    df = pd.read_sql_query(query, main_conn)
    print(f"Loaded {len(df):,} text pairs")
    
    # Create same_author target
    df['same_author'] = (df['source_auth'] == df['target_auth']).astype(int)
    
    # Load SVM scores
    svm_db_path = f"./projects/{project_name}/db/svm.db"
    svm_conn = sqlite3.connect(svm_db_path)
    chapter_df = pd.read_sql_query("SELECT * FROM chapter_assessments", svm_conn)
    svm_conn.close()
    
    # Get text metadata for SVM matching
    text_query = "SELECT text_id, source_filename, chapter_num FROM all_texts"
    text_df = pd.read_sql_query(text_query, main_conn)
    
    def extract_novel_name(filename):
        parts = filename.split('-chapter')[0]
        parts = parts.split('-')[1]
        return parts
    
    text_df['novel'] = text_df['source_filename'].apply(extract_novel_name)
    text_df['number'] = text_df['chapter_num'].astype(str)
    text_lookup = {row['text_id']: (row['novel'], row['number']) 
                   for _, row in text_df.iterrows()}
    
    # Get novel names for SVM columns
    dirs_query = "SELECT id, dir FROM dirs"
    dirs_df = pd.read_sql_query(dirs_query, main_conn)
    novels_dict = {}
    for _, row in dirs_df.iterrows():
        dir_name = row['dir']
        novel_name = dir_name.split('-')[1] if '-' in dir_name else dir_name
        novels_dict[row['id']] = novel_name
    
    main_conn.close()
    
    # Match SVM scores
    print("Matching SVM scores...")
    svm_scores = []
    for idx, row in df.iterrows():
        source_text_id = row['source_text']
        source_auth_id = row['source_auth']
        
        if source_text_id not in text_lookup:
            svm_scores.append(np.nan)
            continue
            
        novel, chapter = text_lookup[source_text_id]
        target_novel = novels_dict.get(source_auth_id, None)
        
        if target_novel is None or target_novel not in chapter_df.columns:
            svm_scores.append(np.nan)
            continue
        
        mask = (chapter_df['novel'] == novel) & (chapter_df['number'] == chapter)
        matching_rows = chapter_df.loc[mask, target_novel]
        
        if len(matching_rows) == 0:
            svm_scores.append(np.nan)
        else:
            svm_scores.append(matching_rows.values[0])
    
    df['svm_score'] = svm_scores
    df = df.dropna(subset=['svm_score'])
    
    print(f"Final dataset: {len(df):,} text pairs with complete data")
    
    return df


def find_anchor_case(df):
    """
    Find the Eliot -> Lawrence anchor case in the data.
    Specifically: 1872-ENG18721—Eliot-chapter_77 -> 1920-ENG19200—Lawrence-chapter_29
    This is our known influence relationship for validation.
    """
    print("\n" + "=" * 70)
    print("STEP 2: FINDING ANCHOR CASE (Eliot ch77 -> Lawrence ch29)")
    print("=" * 70)
    
    # Look for Eliot-Lawrence pairs
    eliot_lawrence = df[
        (df['source_author_name'].str.contains('Eliot', case=False, na=False)) &
        (df['target_author_name'].str.contains('Lawrence', case=False, na=False))
    ]
    
    lawrence_eliot = df[
        (df['source_author_name'].str.contains('Lawrence', case=False, na=False)) &
        (df['target_author_name'].str.contains('Eliot', case=False, na=False))
    ]
    
    anchor_pairs = pd.concat([eliot_lawrence, lawrence_eliot])
    
    if len(anchor_pairs) == 0:
        print("WARNING: No Eliot-Lawrence pairs found in data!")
        return None
    
    print(f"\nFound {len(anchor_pairs)} Eliot-Lawrence pairs")
    
    # Look specifically for Eliot ch77 - Lawrence ch29 pair
    # Exact names: 1872-ENG18721—Eliot-chapter_77 and 1920-ENG19200—Lawrence-chapter_29
    specific_pair = anchor_pairs[
        ((anchor_pairs['source_name'].str.contains('Eliot-chapter_77', case=False, na=False)) &
         (anchor_pairs['target_name'].str.contains('Lawrence-chapter_29', case=False, na=False))) |
        ((anchor_pairs['source_name'].str.contains('Lawrence-chapter_29', case=False, na=False)) &
         (anchor_pairs['target_name'].str.contains('Eliot-chapter_77', case=False, na=False)))
    ]
    
    if len(specific_pair) > 0:
        print("\n*** ANCHOR CASE FOUND: Eliot ch77 <-> Lawrence ch29 ***")
        for _, row in specific_pair.iterrows():
            print(f"\n  Source: {row['source_name']}")
            print(f"  Target: {row['target_name']}")
            print(f"  Hapax Jaccard Distance:     {row['hap_jac_dis']:.6f}")
            print(f"  Alignment Jaccard Distance: {row['al_jac_dis']:.6f}")
            print(f"  SVM Score:                  {row['svm_score']:.6f}")
        return specific_pair
    else:
        print("\nSpecific Eliot ch77 - Lawrence ch29 pair not found.")
        print("Showing top Eliot-Lawrence pairs by combined score:")
        # Show top pairs by combined score
        anchor_pairs['temp_score'] = (anchor_pairs['hap_jac_dis'] + 
                                       anchor_pairs['al_jac_dis'] + 
                                       anchor_pairs['svm_score']) / 3
        top_pairs = anchor_pairs.nlargest(5, 'temp_score')
        for _, row in top_pairs.iterrows():
            print(f"\n  {row['source_name']} -> {row['target_name']}")
            print(f"  Hapax: {row['hap_jac_dis']:.4f}, Align: {row['al_jac_dis']:.4f}, SVM: {row['svm_score']:.4f}")
        return top_pairs


def optimize_weights_for_anchor(df, anchor_case):
    """
    STEP 3: WEIGHT OPTIMIZATION
    
    Find the weight combination that maximizes the anchor case's RANK
    among cross-author pairs.
    
    This is the key insight: we have a KNOWN influence relationship (Eliot -> Lawrence).
    We want to find the weights where this pair stands out MOST from the crowd -
    i.e., where it ranks highest among all cross-author pairs.
    
    We can then apply these calibrated weights to discover unknown influence pairs.
    """
    print("\n" + "=" * 70)
    print("STEP 3: OPTIMIZING WEIGHTS FOR ANCHOR CASE")
    print("=" * 70)
    
    if anchor_case is None or len(anchor_case) == 0:
        print("ERROR: No anchor case found. Cannot optimize weights.")
        return None, None
    
    # Get anchor case info
    anchor_row = anchor_case.iloc[0]
    anchor_pair_id = anchor_row['pair_id']
    anchor_hap = anchor_row['hap_jac_dis']
    anchor_al = anchor_row['al_jac_dis']
    anchor_svm = anchor_row['svm_score']
    
    print(f"\nAnchor case raw scores:")
    print(f"  Hapax Jaccard Distance:     {anchor_hap:.6f}")
    print(f"  Alignment Jaccard Distance: {anchor_al:.6f}")
    print(f"  SVM Score:                  {anchor_svm:.6f}")
    
    # Get cross-author pairs only (these are what we rank against)
    cross_author = df[df['same_author'] == 0].copy()
    n_cross = len(cross_author)
    
    print(f"\nRanking against {n_cross:,} cross-author pairs")
    print("\nSearching weight combinations (step=0.05, sum=1.0)...")
    print("Finding weights where anchor case ranks HIGHEST among cross-author pairs...\n")
    
    best_rank = float('inf')
    best_percentile = 0
    best_weights = None
    all_results = []
    
    step = 0.05
    combinations_tested = 0
    
    for hap_w in np.arange(0.0, 1.01, step):
        for al_w in np.arange(0.0, 1.01 - hap_w, step):
            svm_w = round(1.0 - hap_w - al_w, 2)
            if svm_w < 0:
                continue
            
            combinations_tested += 1
            
            # Calculate scores for ALL cross-author pairs with these weights
            cross_author['temp_score'] = (
                cross_author['hap_jac_dis'] * hap_w +
                cross_author['al_jac_dis'] * al_w +
                cross_author['svm_score'] * svm_w
            )
            
            # Calculate anchor case score
            anchor_score = (anchor_hap * hap_w) + (anchor_al * al_w) + (anchor_svm * svm_w)
            
            # Find anchor case rank (how many pairs score higher?)
            rank = (cross_author['temp_score'] > anchor_score).sum() + 1
            percentile = (1 - rank / n_cross) * 100
            
            all_results.append({
                'hap_weight': round(hap_w, 2),
                'al_weight': round(al_w, 2),
                'svm_weight': round(svm_w, 2),
                'anchor_rank': rank,
                'anchor_percentile': percentile,
                'anchor_score': anchor_score
            })
            
            if rank < best_rank:
                best_rank = rank
                best_percentile = percentile
                best_weights = (round(hap_w, 2), round(al_w, 2), round(svm_w, 2))
    
    # Clean up temp column
    if 'temp_score' in cross_author.columns:
        cross_author.drop('temp_score', axis=1, inplace=True)
    
    results_df = pd.DataFrame(all_results)
    
    print(f"Tested {combinations_tested} weight combinations")
    
    print("\n" + "-" * 50)
    print("OPTIMAL WEIGHTS FOR ANCHOR CASE")
    print("-" * 50)
    print(f"  Hapax weight:     {best_weights[0]:.2f}")
    print(f"  Alignment weight: {best_weights[1]:.2f}")
    print(f"  SVM weight:       {best_weights[2]:.2f}")
    print(f"  Anchor rank:      {best_rank:,} of {n_cross:,}")
    print(f"  Anchor percentile: {best_percentile:.2f}%")
    
    # Show top 10 weight combinations by rank
    print("\n--- TOP 10 WEIGHT COMBINATIONS (by anchor rank) ---")
    top_10 = results_df.nsmallest(10, 'anchor_rank')
    print(f"{'Hapax':>8} {'Align':>8} {'SVM':>8} {'Rank':>10} {'Percentile':>12}")
    print("-" * 50)
    for _, row in top_10.iterrows():
        print(f"{row['hap_weight']:>8.2f} {row['al_weight']:>8.2f} {row['svm_weight']:>8.2f} {int(row['anchor_rank']):>10,} {row['anchor_percentile']:>11.2f}%")
    
    # Interpretation
    print("\n--- INTERPRETATION ---")
    max_var = max(zip(['Hapax (vocabulary)', 'Alignment (phrasing)', 'SVM (style)'], best_weights), key=lambda x: x[1])
    
    if best_weights[0] > best_weights[1] and best_weights[0] > best_weights[2]:
        print("The Eliot-Lawrence pair ranks highest when VOCABULARY (hapax) is weighted most.")
        print("This suggests their influence relationship is primarily visible through")
        print("shared rare word choices - lexical inheritance.")
    elif best_weights[1] > best_weights[0] and best_weights[1] > best_weights[2]:
        print("The Eliot-Lawrence pair ranks highest when PHRASING (alignment) is weighted most.")
        print("This suggests their influence relationship is primarily visible through")
        print("shared sequences and structural patterns in the prose.")
    elif best_weights[2] > best_weights[0] and best_weights[2] > best_weights[1]:
        print("The Eliot-Lawrence pair ranks highest when STYLE (SVM) is weighted most.")
        print("This suggests their influence relationship is primarily visible through")
        print("overall stylometric similarity.")
    else:
        print(f"The optimal weighting is: Hapax={best_weights[0]:.0%}, Align={best_weights[1]:.0%}, SVM={best_weights[2]:.0%}")
        print("This balanced weighting suggests influence operates through multiple channels.")
    
    return best_weights, results_df


def apply_calibrated_weights(df, anchor_case, best_weights):
    """
    STEP 4: APPLY CALIBRATED WEIGHTS
    
    Now that we know which weights maximize our known influence case,
    apply those weights to ALL cross-author pairs to find other influence candidates.
    """
    print("\n" + "=" * 70)
    print("STEP 4: APPLYING CALIBRATED WEIGHTS TO ALL PAIRS")
    print("=" * 70)
    
    if best_weights is None:
        print("ERROR: No calibrated weights available.")
        return None, None
    
    hap_w, al_w, svm_w = best_weights
    
    print(f"\nUsing calibrated weights:")
    print(f"  Hapax:     {hap_w:.2f}")
    print(f"  Alignment: {al_w:.2f}")
    print(f"  SVM:       {svm_w:.2f}")
    
    # Calculate influence score for all pairs
    df['influence_score'] = (
        df['hap_jac_dis'] * hap_w +
        df['al_jac_dis'] * al_w +
        df['svm_score'] * svm_w
    )
    
    # Separate cross-author pairs (these are influence candidates)
    cross_author = df[df['same_author'] == 0].copy()
    same_author = df[df['same_author'] == 1].copy()
    
    print(f"\nScore distribution:")
    print(f"  Cross-author pairs: {len(cross_author):,}")
    print(f"    Min:  {cross_author['influence_score'].min():.6f}")
    print(f"    Max:  {cross_author['influence_score'].max():.6f}")
    print(f"    Mean: {cross_author['influence_score'].mean():.6f}")
    print(f"    Std:  {cross_author['influence_score'].std():.6f}")
    
    print(f"\n  Same-author pairs: {len(same_author):,}")
    print(f"    Min:  {same_author['influence_score'].min():.6f}")
    print(f"    Max:  {same_author['influence_score'].max():.6f}")
    print(f"    Mean: {same_author['influence_score'].mean():.6f}")
    
    # Find anchor case position
    if anchor_case is not None and len(anchor_case) > 0:
        anchor_pair_id = anchor_case.iloc[0]['pair_id']
        anchor_score = df[df['pair_id'] == anchor_pair_id]['influence_score'].values[0]
        
        # Rank among cross-author pairs
        ranked_cross = cross_author.sort_values('influence_score', ascending=False)
        position = ranked_cross['pair_id'].tolist().index(anchor_pair_id) + 1
        percentile = (1 - position / len(ranked_cross)) * 100
        
        print(f"\n--- ANCHOR CASE RANKING ---")
        print(f"Eliot ch77 -> Lawrence ch29")
        print(f"  Influence score: {anchor_score:.6f}")
        print(f"  Rank: {position:,} of {len(ranked_cross):,} cross-author pairs")
        print(f"  Percentile: {percentile:.2f}%")
    
    return df, cross_author


def run_logistic_regression(df):
    """
    STEP 5: Run logistic regression for comparison.
    
    This gives us the statistically-derived coefficients for comparison
    with our calibrated weights from the anchor case.
    """
    print("\n" + "=" * 70)
    print("STEP 5: LOGISTIC REGRESSION (for comparison)")
    print("=" * 70)
    
    predictors = ['hap_jac_dis', 'al_jac_dis', 'svm_score']
    
    X = df[predictors].copy()
    y = df['same_author'].copy()
    X = sm.add_constant(X)
    
    print(f"\nFitting model on {len(y):,} observations")
    print(f"Same author pairs: {y.sum():,} ({y.mean()*100:.2f}%)")
    print(f"Different author pairs: {len(y) - y.sum():,} ({(1-y.mean())*100:.2f}%)")
    
    model = sm.Logit(y, X)
    result = model.fit(disp=0)
    
    print("\n" + "-" * 50)
    print("COEFFICIENTS")
    print("-" * 50)
    print(f"{'Variable':<20} {'Coef':>10} {'Std Err':>10} {'p-value':>12} {'Sig':>5}")
    print("-" * 50)
    
    for var in result.params.index:
        coef = result.params[var]
        se = result.bse[var]
        p = result.pvalues[var]
        sig = '***' if p < 0.001 else '**' if p < 0.01 else '*' if p < 0.05 else ''
        print(f"{var:<20} {coef:>10.4f} {se:>10.4f} {p:>12.2e} {sig:>5}")
    
    print("\n" + "-" * 50)
    print("MODEL FIT")
    print("-" * 50)
    print(f"Pseudo R² (McFadden): {result.prsquared:.4f}")
    print(f"AIC: {result.aic:.2f}")
    print(f"BIC: {result.bic:.2f}")
    
    # Calculate AUC
    y_pred_prob = result.predict(X)
    auc = roc_auc_score(y, y_pred_prob)
    print(f"ROC AUC: {auc:.4f}")
    
    return result, predictors


def rank_influence_candidates(cross_author, anchor_case, best_weights):
    """
    STEP 6: Rank cross-author pairs by influence score.
    These are your influence candidates for literary investigation.
    """
    print("\n" + "=" * 70)
    print("STEP 6: TOP INFLUENCE CANDIDATES")
    print("=" * 70)
    
    # Sort by influence score
    ranked = cross_author.sort_values('influence_score', ascending=False)
    
    print(f"\nUsing calibrated weights: Hapax={best_weights[0]:.2f}, Align={best_weights[1]:.2f}, SVM={best_weights[2]:.2f}")
    print("\n--- TOP 20 INFLUENCE CANDIDATES ---")
    print("(Cross-author pairs most likely to represent literary influence)\n")
    
    for i, (_, row) in enumerate(ranked.head(20).iterrows(), 1):
        print(f"{i:3}. {row['source_author_name']:15} -> {row['target_author_name']:15} "
              f"Score: {row['influence_score']:.4f}")
        print(f"     {row['source_name'][:40]}")
        print(f"     -> {row['target_name'][:40]}")
        print()
    
    # Find anchor case position
    if anchor_case is not None and len(anchor_case) > 0:
        print("\n--- ANCHOR CASE POSITION ---")
        for _, anchor_row in anchor_case.iterrows():
            pair_id = anchor_row['pair_id']
            if pair_id in ranked['pair_id'].values:
                position = ranked['pair_id'].tolist().index(pair_id) + 1
                score = ranked[ranked['pair_id'] == pair_id]['influence_score'].values[0]
                percentile = (1 - position / len(ranked)) * 100
                print(f"\nEliot-Lawrence pair rank: {position:,} of {len(ranked):,}")
                print(f"Influence score: {score:.4f}")
                print(f"Percentile: {percentile:.2f}%")
    
    return ranked


def show_coefficient_interpretation(result, best_weights):
    """
    STEP 7: Compare logistic regression coefficients with calibrated weights.
    """
    print("\n" + "=" * 70)
    print("STEP 7: COMPARING METHODS")
    print("=" * 70)
    
    # Show calibrated weights
    print("\n--- CALIBRATED WEIGHTS (from anchor case) ---")
    if best_weights:
        total = sum(best_weights)
        print(f"  Hapax (vocabulary):     {best_weights[0]:.2f} ({best_weights[0]/total*100:.1f}%)")
        print(f"  Alignment (phrasing):   {best_weights[1]:.2f} ({best_weights[1]/total*100:.1f}%)")
        print(f"  SVM (style):            {best_weights[2]:.2f} ({best_weights[2]/total*100:.1f}%)")
    
    # Show logistic regression coefficients normalized
    print("\n--- LOGISTIC REGRESSION COEFFICIENTS ---")
    
    # Get coefficients (skip intercept)
    coefs = result.params[1:]  # Skip intercept
    abs_coefs = np.abs(coefs)
    total = abs_coefs.sum()
    
    print("(Normalized to show relative importance)\n")
    
    importance = [(var, abs(coef), abs(coef)/total*100) 
                  for var, coef in coefs.items()]
    importance.sort(key=lambda x: x[1], reverse=True)
    
    for var, coef, pct in importance:
        if var == 'hap_jac_dis':
            label = "Hapax (vocabulary)"
        elif var == 'al_jac_dis':
            label = "Alignment (phrasing)"
        else:
            label = "SVM (style)"
        print(f"  {label:<25} {pct:.1f}%")
    
    print("\n--- INTERPRETATION ---")
    if best_weights:
        print(f"Calibrated weights (from known influence case) emphasize:")
        max_idx = best_weights.index(max(best_weights))
        labels = ['VOCABULARY (hapax)', 'PHRASING (alignment)', 'STYLE (SVM)']
        print(f"  -> {labels[max_idx]}")
        print(f"\nThis suggests literary influence in this corpus is best detected")
        print(f"through {labels[max_idx].lower()} similarity.")


def save_results(df, ranked, result, predictors, best_weights):
    """
    STEP 8: Save results for the conference paper.
    """
    print("\n" + "=" * 70)
    print("STEP 8: SAVING RESULTS")
    print("=" * 70)
    
    project_name = get_project_name()
    
    # Save coefficients
    coef_data = {
        'variable': result.params.index.tolist(),
        'coefficient': result.params.values.tolist(),
        'std_error': result.bse.values.tolist(),
        'p_value': result.pvalues.values.tolist(),
        'odds_ratio': np.exp(result.params.values).tolist()
    }
    coef_df = pd.DataFrame(coef_data)
    coef_path = f"./projects/{project_name}/results/influence_coefficients.csv"
    coef_df.to_csv(coef_path, index=False)
    print(f"Coefficients saved to: {coef_path}")
    
    # Save top influence candidates
    top_candidates = ranked.head(100)[['source_author_name', 'target_author_name',
                                        'source_name', 'target_name',
                                        'hap_jac_dis', 'al_jac_dis', 'svm_score',
                                        'influence_score']]
    candidates_path = f"./projects/{project_name}/results/top_influence_candidates.csv"
    top_candidates.to_csv(candidates_path, index=False)
    print(f"Top 100 influence candidates saved to: {candidates_path}")
    
    # Save summary for paper
    summary_path = f"./projects/{project_name}/results/influence_model_summary.txt"
    with open(summary_path, 'w') as f:
        f.write("INFLUENCE DETECTION MODEL SUMMARY\n")
        f.write("=" * 50 + "\n\n")
        
        f.write("CALIBRATED WEIGHTS (from Eliot->Lawrence anchor case)\n")
        f.write("-" * 50 + "\n")
        if best_weights:
            f.write(f"Hapax (vocabulary):     {best_weights[0]:.2f}\n")
            f.write(f"Alignment (phrasing):   {best_weights[1]:.2f}\n")
            f.write(f"SVM (style):            {best_weights[2]:.2f}\n")
        f.write("\n")
        
        f.write("LOGISTIC REGRESSION COEFFICIENTS (for comparison)\n")
        f.write("-" * 50 + "\n")
        for var in result.params.index:
            f.write(f"{var}: {result.params[var]:.6f} (p={result.pvalues[var]:.2e})\n")
        f.write(f"\nPseudo R²: {result.prsquared:.4f}\n")
        f.write(f"AIC: {result.aic:.2f}\n")
    print(f"Summary saved to: {summary_path}")


def main():
    """
    Main function for influence detection analysis.
    """
    print("\n" + "=" * 70)
    print("LITERARY INFLUENCE DETECTION")
    print("Calibrated via Known Influence Case")
    print("=" * 70)
    print("\nMethod:")
    print("1. Use a KNOWN influence relationship as calibration (Eliot -> Lawrence)")
    print("2. Find weights that maximize this anchor case's score")
    print("3. Apply those weights to discover OTHER influence candidates")
    print("\nAnchor case: George Eliot (Middlemarch ch77) -> D.H. Lawrence (The Rainbow ch29)")
    
    # Step 1: Load data
    df = load_data()
    
    # Step 2: Find anchor case
    anchor_case = find_anchor_case(df)
    
    # Step 3: Optimize weights for anchor case
    best_weights, weight_results = optimize_weights_for_anchor(df, anchor_case)
    
    # Step 4: Apply calibrated weights to all pairs
    df, cross_author = apply_calibrated_weights(df, anchor_case, best_weights)
    
    # Step 5: Run logistic regression for comparison
    result, predictors = run_logistic_regression(df)
    
    # Step 6: Rank influence candidates
    ranked = rank_influence_candidates(cross_author, anchor_case, best_weights)
    
    # Step 7: Compare methods
    show_coefficient_interpretation(result, best_weights)
    
    # Step 8: Save results
    save_results(df, ranked, result, predictors, best_weights)
    
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print("\nFor the conference paper:")
    print("1. Report the calibrated weights from the Eliot-Lawrence anchor case")
    print("2. Explain what these weights reveal about the nature of influence")
    print("3. Present top influence candidates for literary investigation")
    print("4. Note: logistic regression coefficients provided for comparison")
    print()


if __name__ == "__main__":
    main()