# -*- coding: utf-8 -*-
import pandas as pd
import json
import os
import warnings
from datetime import datetime, timedelta
warnings.filterwarnings('ignore')

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

###############################################################################
# 1. Load & filter data
###############################################################################
def load_all_data():
    files_info = {
        'Airペイ': ['260331_CAD_Airぺイ.csv', '260406_CAD_Airペイ.csv'],
        'SWE_NonRTG': ['260331_CAD_SWE_NonRTG.csv', '260406_CAD_SWE_NonRTG.csv'],
        'SWE_RTG': ['260331_CAD_SWE_RTG.csv', '260406_CAD_SWE_RTG.csv'],
    }
    all_dfs = []
    for dataset_name, file_list in files_info.items():
        for fname in file_list:
            fpath = os.path.join(DATA_DIR, fname)
            if not os.path.exists(fpath):
                continue
            df = pd.read_csv(fpath, sep='\t', encoding='utf-16-le', low_memory=False)
            df['dataset'] = dataset_name
            df['source_file'] = fname
            all_dfs.append(df)
            print(f"  Loaded {fname}: {len(df)} rows")
    return all_dfs

def normalize_and_combine(all_dfs):
    common_cols = [
        'client_name','media','account_name','campaign_id','campaign_name',
        'adgroup_id','adgroup_name','ad_id','ad_name',
        'report_date','report_month','report_week','week','day_of_the_week',
        'Device','ad_type','status','title','text','image_url','video_url',
        'cost','impression','click','ctr','cpc','dataset','source_file'
    ]
    swe_extra = ['conversion','cvr','cpa','cpm',
                 'ターゲティング','管理区分',
                 '全商品','主要商品','STEP-A','STEP-B',
                 'デバイス-A','デバイス-B',
                 'category1','category2','category3','category4','category5',
                 'category6','category7','category8','category9','category10',
                 'label1','label2','label3','label4','label5']
    combined = []
    for df in all_dfs:
        row = {}
        for c in common_cols:
            row[c] = df[c] if c in df.columns else None
        temp = pd.DataFrame(row)
        for c in swe_extra:
            temp[c] = df[c].values if c in df.columns else None
        ds = df['dataset'].iloc[0] if len(df) > 0 else ''
        if ds == 'Airペイ':
            cv_col = df.columns[73]
            temp['conversion'] = pd.to_numeric(df[cv_col], errors='coerce').fillna(0)
            if 'KPI別一覧' in df.columns:
                temp['category1'] = df['KPI別一覧']
            if '指標/被指標' in df.columns:
                temp['category2'] = df['指標/被指標']
        combined.append(temp)
    result = pd.concat(combined, ignore_index=True)
    for c in ['cost','impression','click','ctr','cpc','conversion','cvr','cpa','cpm']:
        if c in result.columns:
            result[c] = pd.to_numeric(result[c], errors='coerce').fillna(0)
    result['report_date'] = pd.to_datetime(result['report_date'], errors='coerce')
    result = result.dropna(subset=['report_date'])
    return result

def split_data(df):
    """Split into 6 datasets: YDA x (Airpay, onpr通常, onpr美容, lcmr) + GDN x (Airpay, onpr美容)"""
    ydn = df[df['media'].str.contains('YDN', case=False, na=False)].copy()
    gdn = df[df['media'].str.contains('GDN', case=False, na=False)].copy()

    # --- YDA splits ---
    airpay_ydn_all = ydn[ydn['dataset'] == 'Airペイ'].copy()
    exclude_mask = airpay_ydn_all['campaign_name'].str.contains('CASM|SBI|テミクス', case=False, na=False)
    airpay_yda = airpay_ydn_all[~exclude_mask].copy()  # Airペイ本体のみ

    swe_yda = ydn[ydn['dataset'].isin(['SWE_RTG','SWE_NonRTG'])].copy()
    onpr_yda = swe_yda[swe_yda['campaign_name'].str.contains('onpr', case=False, na=False)].copy()
    onpr_biyou_yda = onpr_yda[onpr_yda['campaign_name'].str.contains('美容', na=False)].copy()
    onpr_normal_yda = onpr_yda[~onpr_yda['campaign_name'].str.contains('美容', na=False)].copy()
    lcmr_yda = swe_yda[swe_yda['campaign_name'].str.contains('lcmr', case=False, na=False)].copy()

    # --- GDN splits ---
    airpay_gdn_all = gdn[gdn['dataset'] == 'Airペイ'].copy()
    exclude_gdn = airpay_gdn_all['campaign_name'].str.contains('CASM|SBI|テミクス', case=False, na=False)
    airpay_gdn = airpay_gdn_all[~exclude_gdn].copy()

    swe_gdn = gdn[gdn['dataset'].isin(['SWE_RTG','SWE_NonRTG'])].copy()
    onpr_biyou_gdn = swe_gdn[
        swe_gdn['campaign_name'].str.contains('onpr', case=False, na=False) &
        swe_gdn['campaign_name'].str.contains('美容', na=False)
    ].copy()

    return {
        'airpay_yda':       ('Airペイ × YDA',          airpay_yda,      '#1a73e8', '#174ea6'),
        'onpare_yda':       ('SWE オンパレ通常 × YDA', onpr_normal_yda, '#0f9d58', '#0b7a42'),
        'onpare_biyou_yda': ('SWE オンパレ美容 × YDA', onpr_biyou_yda,  '#ab47bc', '#8e24aa'),
        'locomoa_yda':      ('SWE ロコモア × YDA',     lcmr_yda,        '#db4437', '#b33629'),
        'airpay_gdn':       ('Airペイ × GDN',          airpay_gdn,      '#00acc1', '#00838f'),
        'onpare_biyou_gdn': ('SWE オンパレ美容 × GDN', onpr_biyou_gdn,  '#ff7043', '#e64a19'),
    }

###############################################################################
# 2. Analysis helpers
###############################################################################
def agg_metrics(df):
    d = {
        'cost': df['cost'].sum(),
        'impression': df['impression'].sum(),
        'click': df['click'].sum(),
        'conversion': df['conversion'].sum() if 'conversion' in df.columns else 0,
    }
    d['ctr'] = (d['click'] / d['impression'] * 100) if d['impression'] > 0 else 0
    d['cpc'] = (d['cost'] / d['click']) if d['click'] > 0 else 0
    d['cpa'] = (d['cost'] / d['conversion']) if d['conversion'] > 0 else 0
    d['cpm'] = (d['cost'] / d['impression'] * 1000) if d['impression'] > 0 else 0
    return d

def analyze_by_group(df, group_cols, top_n=30):
    results = []
    for name, group in df.groupby(group_cols):
        m = agg_metrics(group)
        if isinstance(name, tuple):
            for i, col in enumerate(group_cols):
                m[col] = name[i]
        else:
            m[group_cols[0]] = name
        results.append(m)
    rdf = pd.DataFrame(results)
    if len(rdf) > 0:
        rdf = rdf.sort_values('cost', ascending=False).head(top_n)
    return rdf

###############################################################################
# 3. HTML generator
###############################################################################
def fmt_num(v, decimals=0):
    if v == 0 or pd.isna(v): return '-'
    return f'{v:,.{decimals}f}'
def fmt_pct(v):
    if v == 0 or pd.isna(v): return '-'
    return f'{v:.2f}%'
def fmt_yen(v):
    if v == 0 or pd.isna(v): return '-'
    return f'¥{v:,.0f}'

TARGET_CPA_MAP = {
    'Airペイ × YDA': 38461,
    'SWE オンパレ通常 × YDA': 2500,
    'SWE オンパレ美容 × YDA': 2500,
    'SWE オンパレ美容 × GDN': 2500,
    'SWE ロコモア × YDA': 11000,
    'Airペイ × GDN': 38461,
}

def find_seisa_targets(df, target_cpa, threshold_pct=1.3, min_days=10):
    """Find items with CPA >= threshold AND 10+ days delivery at campaign/adgroup/ad level."""
    threshold = target_cpa * threshold_pct
    results = {}
    for level, col in [('campaign', 'campaign_name'), ('adgroup', 'adgroup_name'), ('ad', 'ad_name')]:
        flagged = []
        for name, g in df.groupby(col):
            if pd.isna(name) or str(name).strip() == '':
                continue
            days = g['report_date'].nunique()
            cost = g['cost'].sum()
            cv = g['conversion'].sum() if 'conversion' in g.columns else 0
            click = g['click'].sum()
            imp = g['impression'].sum()
            if cost == 0:
                continue
            cpa = cost / cv if cv > 0 else 999999
            cvr = (cv / click * 100) if click > 0 else 0
            ctr = (click / imp * 100) if imp > 0 else 0
            ratio = (cpa / target_cpa * 100) if target_cpa > 0 else 0

            if days >= min_days and cpa >= threshold:
                flagged.append({
                    'name': name, 'cost': cost, 'cv': cv, 'cpa': cpa,
                    'days': days, 'click': click, 'cvr': cvr, 'ctr': ctr,
                    'ratio': ratio, 'imp': imp,
                })
        flagged.sort(key=lambda x: -x['cost'])
        results[level] = flagged
    return results

def generate_report(report_name, df, color1, color2):
    if len(df) == 0:
        return '<html><body><h1>データなし</h1></body></html>'

    today = df['report_date'].max()
    three_months_ago = today - timedelta(days=90)
    three_weeks_ago = today - timedelta(days=21)
    df_3m = df[df['report_date'] >= three_months_ago]
    df_3w = df[df['report_date'] >= three_weeks_ago]

    # ---- Aggregations ----
    total_3m = agg_metrics(df_3m)
    total_3w = agg_metrics(df_3w)

    # Monthly
    monthly = analyze_by_group(df_3m, ['report_month'])
    if 'report_month' in monthly.columns:
        monthly = monthly.sort_values('report_month')

    # Daily (3w)
    df_3w_c = df_3w.copy()
    df_3w_c['date_str'] = df_3w_c['report_date'].dt.strftime('%Y-%m-%d')
    daily = analyze_by_group(df_3w_c, ['date_str'])
    if 'date_str' in daily.columns:
        daily = daily.sort_values('date_str')

    # RTG/NonRTG split
    by_dataset = analyze_by_group(df_3m, ['dataset'])

    # Campaign
    by_campaign = analyze_by_group(df_3w, ['campaign_name'], top_n=40)

    # Adgroup
    by_adgroup = analyze_by_group(df_3w, ['campaign_name','adgroup_name'], top_n=50)

    # Ad
    by_ad = analyze_by_group(df_3w, ['adgroup_name','ad_name'], top_n=40)

    # Targeting
    by_targeting = pd.DataFrame()
    if 'ターゲティング' in df_3w.columns:
        tgt = df_3w[df_3w['ターゲティング'].notna() & (df_3w['ターゲティング'] != '')]
        if len(tgt) > 0:
            by_targeting = analyze_by_group(tgt, ['ターゲティング'], top_n=30)

    # Management category
    by_mgmt = pd.DataFrame()
    if '管理区分' in df_3w.columns:
        mgmt = df_3w[df_3w['管理区分'].notna() & (df_3w['管理区分'] != '')]
        if len(mgmt) > 0:
            by_mgmt = analyze_by_group(mgmt, ['管理区分'], top_n=20)

    # Device
    by_device = analyze_by_group(df_3w, ['Device'], top_n=10)

    # Creative
    by_creative = pd.DataFrame()
    cr_data = df_3w[df_3w['ad_name'].notna() & (df_3w['ad_name'] != '')]
    if len(cr_data) > 0:
        cr_results = []
        for ad_name, group in cr_data.groupby('ad_name'):
            m = agg_metrics(group)
            m['ad_name'] = ad_name
            m['title'] = group['title'].dropna().iloc[0] if group['title'].notna().any() else ''
            m['text'] = group['text'].dropna().iloc[0] if group['text'].notna().any() else ''
            m['ad_type'] = group['ad_type'].dropna().iloc[0] if group['ad_type'].notna().any() else ''
            m['image_url'] = group['image_url'].dropna().iloc[0] if group['image_url'].notna().any() else ''
            cr_results.append(m)
        by_creative = pd.DataFrame(cr_results).sort_values('cost', ascending=False).head(30)

    # Priority
    priority_data = by_campaign.copy() if len(by_campaign) > 0 else pd.DataFrame()
    if len(priority_data) > 0:
        mx_cost = priority_data['cost'].max() or 1
        mx_conv = priority_data['conversion'].max() or 1
        priority_data['cost_score'] = priority_data['cost'] / mx_cost * 50
        q90 = priority_data['cpa'].quantile(0.9)
        priority_data['efficiency_score'] = (1 - priority_data['cpa'].clip(upper=q90) / q90) * 30 if q90 > 0 else 0
        priority_data['volume_score'] = priority_data['conversion'] / mx_conv * 20
        priority_data['priority_score'] = priority_data['cost_score'] + priority_data['efficiency_score'] + priority_data['volume_score']
        priority_data = priority_data.sort_values('priority_score', ascending=False)

    # ---- JSON for charts ----
    def to_j(series): return json.dumps(series.tolist() if hasattr(series,'tolist') else series, ensure_ascii=False, default=str)
    def to_jn(series, dec=0):
        return json.dumps([round(float(x), dec) for x in series] if hasattr(series,'tolist') else [], ensure_ascii=False)

    ml = to_j(monthly.get('report_month', pd.Series()))
    mc = to_jn(monthly.get('cost', pd.Series()))
    mcv = to_jn(monthly.get('conversion', pd.Series()), 1)
    mcpa = to_jn(monthly.get('cpa', pd.Series()))

    dl = to_j(daily.get('date_str', pd.Series()))
    dcost = to_jn(daily.get('cost', pd.Series()))
    dclick = to_jn(daily.get('click', pd.Series()))
    dcv = to_jn(daily.get('conversion', pd.Series()), 1)
    dcpa = to_jn(daily.get('cpa', pd.Series()))

    ds_labels = to_j(by_dataset.get('dataset', pd.Series()))
    ds_cost = to_jn(by_dataset.get('cost', pd.Series()))

    dev_labels = to_j(by_device.get('Device', pd.Series()))
    dev_cost = to_jn(by_device.get('cost', pd.Series()))

    tgt_labels = to_j(by_targeting.get('ターゲティング', pd.Series()))
    tgt_cost = to_jn(by_targeting.get('cost', pd.Series()))

    # ---- Table builders ----
    def trunc(s, n):
        s = str(s)
        return s[:n-3]+'...' if len(s)>n else s

    campaign_rows = ''
    for _, r in by_campaign.iterrows():
        campaign_rows += f'''<tr>
          <td title="{r.get('campaign_name','')}">{trunc(r.get('campaign_name',''),65)}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_pct(r['ctr'])}</td>
          <td class="num">{fmt_yen(r['cpc'])}</td><td class="num">{fmt_num(r['conversion'],1)}</td>
          <td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    adgroup_rows = ''
    for _, r in by_adgroup.iterrows():
        adgroup_rows += f'''<tr>
          <td title="{r.get('campaign_name','')}">{trunc(r.get('campaign_name',''),45)}</td>
          <td title="{r.get('adgroup_name','')}">{trunc(r.get('adgroup_name',''),55)}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_pct(r['ctr'])}</td>
          <td class="num">{fmt_num(r['conversion'],1)}</td><td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    ad_rows = ''
    for _, r in by_ad.iterrows():
        ad_rows += f'''<tr>
          <td title="{r.get('adgroup_name','')}">{trunc(r.get('adgroup_name',''),40)}</td>
          <td title="{r.get('ad_name','')}">{trunc(r.get('ad_name',''),60)}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_num(r['conversion'],1)}</td>
          <td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    targeting_rows = ''
    for _, r in by_targeting.iterrows():
        targeting_rows += f'''<tr>
          <td>{r.get('ターゲティング','')}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_pct(r['ctr'])}</td>
          <td class="num">{fmt_num(r['conversion'],1)}</td><td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    mgmt_rows = ''
    for _, r in by_mgmt.iterrows():
        mgmt_rows += f'''<tr>
          <td>{r.get('管理区分','')}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_num(r['conversion'],1)}</td>
          <td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    creative_rows = ''
    for _, r in by_creative.iterrows():
        img = str(r.get('image_url',''))
        img_tag = f'<img src="{img}" style="max-width:60px;max-height:40px;" onerror="this.style.display=\'none\'">' if img and img not in ('','nan') else '-'
        creative_rows += f'''<tr>
          <td title="{r.get('ad_name','')}">{trunc(r.get('ad_name',''),50)}</td>
          <td>{r.get('ad_type','')}</td>
          <td title="{r.get('title','')}">{trunc(r.get('title',''),40)}</td>
          <td title="{r.get('text','')}">{trunc(r.get('text',''),40)}</td>
          <td>{img_tag}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['impression'])}</td>
          <td class="num">{fmt_num(r['click'])}</td><td class="num">{fmt_pct(r['ctr'])}</td>
          <td class="num">{fmt_num(r['conversion'],1)}</td><td class="num">{fmt_yen(r['cpa'])}</td></tr>'''

    priority_rows = ''
    for rank, (_, r) in enumerate(priority_data.iterrows(), 1):
        badge = '🔴' if rank <= 3 else '🟡' if rank <= 10 else '🟢'
        priority_rows += f'''<tr>
          <td>{badge} {rank}</td>
          <td title="{r.get('campaign_name','')}">{trunc(r.get('campaign_name',''),55)}</td>
          <td class="num">{fmt_yen(r['cost'])}</td><td class="num">{fmt_num(r['conversion'],1)}</td>
          <td class="num">{fmt_yen(r['cpa'])}</td><td class="num">{fmt_num(r.get('priority_score',0),1)}</td></tr>'''

    # ---- Build HTML ----
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{report_name} レポート</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
  :root {{ --primary:{color1}; --primary-dark:{color2}; --success:#0f9d58; --warning:#f4b400; --danger:#db4437; --bg:#f8f9fa; --card-bg:#fff; --text:#202124; --text2:#5f6368; --border:#e0e0e0; }}
  * {{ margin:0;padding:0;box-sizing:border-box; }}
  body {{ font-family:'Segoe UI','Hiragino Kaku Gothic ProN','Meiryo',sans-serif; background:var(--bg); color:var(--text); line-height:1.6; }}
  .header {{ background:linear-gradient(135deg,{color1},{color2}); color:#fff; padding:28px 32px; }}
  .header h1 {{ font-size:22px; font-weight:600; }}
  .header p {{ opacity:0.85; font-size:13px; margin-top:4px; }}
  .container {{ max-width:1400px; margin:0 auto; padding:20px; }}
  .nav {{ position:sticky;top:0;z-index:100;background:#fff;border-bottom:1px solid var(--border);padding:10px 20px;display:flex;gap:6px;flex-wrap:wrap;box-shadow:0 2px 4px rgba(0,0,0,0.06); }}
  .nav a {{ text-decoration:none;color:var(--text2);padding:6px 14px;border-radius:20px;font-size:13px;font-weight:500;transition:all .2s;white-space:nowrap; }}
  .nav a:hover,.nav a.active {{ background:var(--primary);color:#fff; }}
  .kpi-grid {{ display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:14px;margin:16px 0; }}
  .kpi-card {{ background:var(--card-bg);border-radius:12px;padding:18px;box-shadow:0 1px 3px rgba(0,0,0,0.08);text-align:center; }}
  .kpi-card .label {{ font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:.5px; }}
  .kpi-card .value {{ font-size:20px;font-weight:700;margin-top:4px;color:var(--primary); }}
  .kpi-card .sub {{ font-size:11px;color:var(--text2);margin-top:2px; }}
  .section {{ margin:28px 0; }}
  .section h2 {{ font-size:17px;font-weight:600;margin-bottom:14px;padding-bottom:6px;border-bottom:2px solid var(--primary); }}
  .chart-row {{ display:grid;grid-template-columns:repeat(auto-fit,minmax(380px,1fr));gap:16px;margin:14px 0; }}
  .chart-card {{ background:var(--card-bg);border-radius:12px;padding:18px;box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
  .chart-card h3 {{ font-size:13px;color:var(--text2);margin-bottom:10px; }}
  .chart-card canvas {{ max-height:300px; }}
  table {{ width:100%;border-collapse:collapse;font-size:12px;background:var(--card-bg);border-radius:8px;overflow:hidden;box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
  th {{ background:#f1f3f4;padding:9px 7px;text-align:left;font-weight:600;font-size:11px;color:var(--text2);position:sticky;top:0;white-space:nowrap; }}
  td {{ padding:7px;border-top:1px solid #f1f3f4;max-width:280px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap; }}
  td.num {{ text-align:right;font-variant-numeric:tabular-nums; }}
  tr:hover {{ background:#f8f9ff; }}
  .table-wrap {{ overflow-x:auto;max-height:520px;overflow-y:auto;border-radius:8px; }}
  .tab-btns {{ display:flex;gap:4px;margin-bottom:10px; }}
  .tab-btn {{ padding:7px 14px;border:1px solid var(--border);background:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:500;transition:all .2s; }}
  .tab-btn.active {{ background:var(--primary);color:#fff;border-color:var(--primary); }}
  .tab-content {{ display:none; }}
  .tab-content.active {{ display:block; }}
  @media(max-width:768px) {{ .chart-row{{grid-template-columns:1fr;}} .kpi-grid{{grid-template-columns:repeat(2,1fr);}} }}
</style>
</head>
<body>
<div class="header">
  <h1>{report_name} パフォーマンスレポート</h1>
  <p>データ期間: {df['report_date'].min().strftime('%Y-%m-%d')} 〜 {today.strftime('%Y-%m-%d')} | 媒体: YDA (Yahoo!ディスプレイ) | 生成: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</div>
<nav class="nav">
  <a href="#summary" class="active">サマリ</a><a href="#monthly">月別</a><a href="#daily">日別</a>
  <a href="#campaign">キャンペーン</a><a href="#adgroup">アドグループ</a><a href="#ad">アド</a>
  <a href="#targeting">ターゲティング</a><a href="#creative">クリエイティブ</a><a href="#priority">優先順位</a><a href="#seisa" style="background:#dc2626;color:#fff;">🔍 精査対象</a>
</nav>
<div class="container">

<!-- SUMMARY -->
<section id="summary" class="section">
  <h2>📊 全体サマリ</h2>
  <p style="font-size:12px;color:var(--text2);margin-bottom:10px;">直近3ヶ月 (YDA)</p>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="label">総費用</div><div class="value">{fmt_yen(total_3m['cost'])}</div><div class="sub">3ヶ月</div></div>
    <div class="kpi-card"><div class="label">IMP</div><div class="value">{fmt_num(total_3m['impression'])}</div></div>
    <div class="kpi-card"><div class="label">Click</div><div class="value">{fmt_num(total_3m['click'])}</div></div>
    <div class="kpi-card"><div class="label">CTR</div><div class="value">{fmt_pct(total_3m['ctr'])}</div></div>
    <div class="kpi-card"><div class="label">CPC</div><div class="value">{fmt_yen(total_3m['cpc'])}</div></div>
    <div class="kpi-card"><div class="label">CV</div><div class="value">{fmt_num(total_3m['conversion'],1)}</div></div>
    <div class="kpi-card"><div class="label">CPA</div><div class="value">{fmt_yen(total_3m['cpa'])}</div></div>
    <div class="kpi-card"><div class="label">CPM</div><div class="value">{fmt_yen(total_3m['cpm'])}</div></div>
  </div>
  <p style="font-size:12px;color:var(--text2);margin:12px 0 8px;">直近3週間</p>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="label">費用(3W)</div><div class="value">{fmt_yen(total_3w['cost'])}</div></div>
    <div class="kpi-card"><div class="label">IMP(3W)</div><div class="value">{fmt_num(total_3w['impression'])}</div></div>
    <div class="kpi-card"><div class="label">Click(3W)</div><div class="value">{fmt_num(total_3w['click'])}</div></div>
    <div class="kpi-card"><div class="label">CV(3W)</div><div class="value">{fmt_num(total_3w['conversion'],1)}</div></div>
    <div class="kpi-card"><div class="label">CPA(3W)</div><div class="value">{fmt_yen(total_3w['cpa'])}</div></div>
    <div class="kpi-card"><div class="label">CTR(3W)</div><div class="value">{fmt_pct(total_3w['ctr'])}</div></div>
  </div>
</section>

<!-- MONTHLY -->
<section id="monthly" class="section">
  <h2>📅 月別推移</h2>
  <div class="chart-row">
    <div class="chart-card"><h3>費用 & CV数</h3><canvas id="mChart1"></canvas></div>
    <div class="chart-card"><h3>CPA推移</h3><canvas id="mChart2"></canvas></div>
  </div>
</section>

<!-- DAILY -->
<section id="daily" class="section">
  <h2>📈 日別推移（直近3週間）</h2>
  <div class="chart-row">
    <div class="chart-card"><h3>費用 & クリック</h3><canvas id="dChart1"></canvas></div>
    <div class="chart-card"><h3>CV & CPA</h3><canvas id="dChart2"></canvas></div>
  </div>
</section>

<!-- CAMPAIGN -->
<section id="campaign" class="section">
  <h2>🎯 キャンペーン別（直近3週間）</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>キャンペーン名</th><th>費用</th><th>IMP</th><th>Click</th><th>CTR</th><th>CPC</th><th>CV</th><th>CPA</th></tr></thead>
    <tbody>{campaign_rows}</tbody>
  </table></div>
</section>

<!-- ADGROUP -->
<section id="adgroup" class="section">
  <h2>📁 アドグループ別（直近3週間）</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>キャンペーン</th><th>アドグループ</th><th>費用</th><th>IMP</th><th>Click</th><th>CTR</th><th>CV</th><th>CPA</th></tr></thead>
    <tbody>{adgroup_rows}</tbody>
  </table></div>
</section>

<!-- AD -->
<section id="ad" class="section">
  <h2>📝 アド別（直近3週間）</h2>
  <div class="table-wrap"><table>
    <thead><tr><th>アドグループ</th><th>アド名</th><th>費用</th><th>IMP</th><th>Click</th><th>CV</th><th>CPA</th></tr></thead>
    <tbody>{ad_rows}</tbody>
  </table></div>
</section>

<!-- TARGETING -->
<section id="targeting" class="section">
  <h2>🎯 ターゲティング・管理区分</h2>
  <div class="chart-row">
    <div class="chart-card"><h3>RTG/NonRTG 費用</h3><canvas id="dsChart"></canvas></div>
    <div class="chart-card"><h3>ターゲティング別 費用</h3><canvas id="tgtChart"></canvas></div>
  </div>
  <div style="margin-top:16px;">
    <div class="tab-btns">
      <button class="tab-btn active" onclick="swTab(event,'tgt-t')">ターゲティング別</button>
      <button class="tab-btn" onclick="swTab(event,'mgmt-t')">管理区分別</button>
    </div>
    <div id="tgt-t" class="tab-content active">
      <div class="table-wrap"><table>
        <thead><tr><th>ターゲティング</th><th>費用</th><th>IMP</th><th>Click</th><th>CTR</th><th>CV</th><th>CPA</th></tr></thead>
        <tbody>{targeting_rows}</tbody>
      </table></div>
    </div>
    <div id="mgmt-t" class="tab-content">
      <div class="table-wrap"><table>
        <thead><tr><th>管理区分</th><th>費用</th><th>IMP</th><th>Click</th><th>CV</th><th>CPA</th></tr></thead>
        <tbody>{mgmt_rows}</tbody>
      </table></div>
    </div>
  </div>
</section>

<!-- CREATIVE -->
<section id="creative" class="section">
  <h2>🎨 クリエイティブ分析（直近3週間）</h2>
  <div class="chart-row">
    <div class="chart-card"><h3>デバイス別費用</h3><canvas id="devChart"></canvas></div>
  </div>
  <div class="table-wrap" style="margin-top:14px;"><table>
    <thead><tr><th>アド名</th><th>タイプ</th><th>タイトル</th><th>テキスト</th><th>画像</th><th>費用</th><th>IMP</th><th>Click</th><th>CTR</th><th>CV</th><th>CPA</th></tr></thead>
    <tbody>{creative_rows}</tbody>
  </table></div>
</section>

<!-- PRIORITY -->
<section id="priority" class="section">
  <h2>⚡ 優先順位</h2>
  <p style="font-size:11px;color:var(--text2);margin-bottom:10px;">スコア = 費用規模(50%) + 効率性(30%) + CV量(20%)</p>
  <div class="table-wrap"><table>
    <thead><tr><th>順位</th><th>キャンペーン</th><th>費用</th><th>CV</th><th>CPA</th><th>スコア</th></tr></thead>
    <tbody>{priority_rows}</tbody>
  </table></div>
</section>

<!-- SEISA -->
<section id="seisa" class="section">
  <h2 style="color:#dc2626;">🔍 精査対象ピックアップ</h2>
  <p style="font-size:12px;color:var(--text2);margin-bottom:14px;padding:10px;background:#fef2f2;border-radius:8px;border-left:4px solid #dc2626;">
    条件: <strong>CPA が目標の130%以上</strong> かつ <strong>配信10日以上</strong> のキャンペーン・アドグループ・アドを自動抽出。
    目標CPA: <strong>__SEISA_TGT__</strong> → 精査閾値: <strong>__SEISA_THR__</strong>
  </p>
  __SEISA_HTML__
</section>

</div>
<script>
const C=['#1a73e8','#db4437','#f4b400','#0f9d58','#ab47bc','#00acc1','#ff7043','#5c6bc0','#26a69a','#ec407a','#7e57c2','#42a5f5'];
function swTab(e,id)__LBRACE__ document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active')); document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active')); document.getElementById(id).classList.add('active'); e.target.classList.add('active'); __RBRACE__
document.querySelectorAll('.nav a').forEach(a=>__LBRACE__ a.addEventListener('click',function(e)__LBRACE__ e.preventDefault(); document.querySelector(this.getAttribute('href')).scrollIntoView(__LBRACE__behavior:'smooth'__RBRACE__); document.querySelectorAll('.nav a').forEach(x=>x.classList.remove('active')); this.classList.add('active'); __RBRACE__); __RBRACE__);
</script>
__CHARTS_SCRIPT__
</body></html>'''
    # Fix JS braces and inject chart script
    html = html.replace('__LBRACE__', '{').replace('__RBRACE__', '}')

    charts_js = """<script>
new Chart(document.getElementById('mChart1'),{type:'bar',data:{labels:__ML__,datasets:[{label:'費用(¥)',data:__MC__,backgroundColor:'rgba(26,115,232,0.7)',yAxisID:'y',borderRadius:4},{label:'CV',data:__MCV__,type:'line',borderColor:'#db4437',yAxisID:'y1',tension:.3,pointRadius:5}]},options:{responsive:true,interaction:{mode:'index',intersect:false},scales:{y:{position:'left',title:{display:true,text:'費用'},ticks:{callback:v=>'¥'+(v/1e6).toFixed(1)+'M'}},y1:{position:'right',title:{display:true,text:'CV'},grid:{drawOnChartArea:false}}}}});
new Chart(document.getElementById('mChart2'),{type:'bar',data:{labels:__ML__,datasets:[{label:'CPA(¥)',data:__MCPA__,backgroundColor:'rgba(244,180,0,0.7)',borderRadius:4}]},options:{responsive:true,scales:{y:{ticks:{callback:v=>'¥'+v.toLocaleString()}}}}});
new Chart(document.getElementById('dChart1'),{type:'line',data:{labels:__DL__,datasets:[{label:'費用',data:__DCOST__,borderColor:'#1a73e8',fill:true,backgroundColor:'rgba(26,115,232,0.06)',yAxisID:'y',tension:.3},{label:'Click',data:__DCLICK__,borderColor:'#0f9d58',fill:true,backgroundColor:'rgba(15,157,88,0.06)',yAxisID:'y1',tension:.3}]},options:{responsive:true,interaction:{mode:'index',intersect:false},scales:{x:{ticks:{maxRotation:45,font:{size:9}}},y:{position:'left',ticks:{callback:v=>'¥'+(v/1e4).toFixed(0)+'万'}},y1:{position:'right',grid:{drawOnChartArea:false}}}}});
new Chart(document.getElementById('dChart2'),{type:'bar',data:{labels:__DL__,datasets:[{label:'CV',data:__DCV__,backgroundColor:'rgba(219,68,55,0.6)',yAxisID:'y',borderRadius:3},{label:'CPA',data:__DCPA__,type:'line',borderColor:'#f4b400',yAxisID:'y1',tension:.3,pointRadius:3}]},options:{responsive:true,interaction:{mode:'index',intersect:false},scales:{x:{ticks:{maxRotation:45,font:{size:9}}},y:{position:'left',title:{display:true,text:'CV'}},y1:{position:'right',grid:{drawOnChartArea:false},ticks:{callback:v=>'¥'+v.toLocaleString()}}}}});
new Chart(document.getElementById('dsChart'),{type:'doughnut',data:{labels:__DSL__,datasets:[{data:__DSC__,backgroundColor:C.slice(0,5)}]},options:{responsive:true,plugins:{legend:{position:'right'}}}});
new Chart(document.getElementById('tgtChart'),{type:'doughnut',data:{labels:__TGTL__,datasets:[{data:__TGTC__,backgroundColor:C.slice(3,12)}]},options:{responsive:true,plugins:{legend:{position:'right'}}}});
new Chart(document.getElementById('devChart'),{type:'bar',data:{labels:__DEVL__,datasets:[{label:'費用',data:__DEVC__,backgroundColor:C.slice(0,5),borderRadius:6}]},options:{responsive:true,indexAxis:'y',plugins:{legend:{display:false}},scales:{x:{ticks:{callback:v=>'¥'+(v/1e4).toFixed(0)+'万'}}}}});
</script>"""

    charts_js = (charts_js
        .replace('__ML__', ml).replace('__MC__', mc).replace('__MCV__', mcv).replace('__MCPA__', mcpa)
        .replace('__DL__', dl).replace('__DCOST__', dcost).replace('__DCLICK__', dclick)
        .replace('__DCV__', dcv).replace('__DCPA__', dcpa)
        .replace('__DSL__', ds_labels).replace('__DSC__', ds_cost)
        .replace('__TGTL__', tgt_labels).replace('__TGTC__', tgt_cost)
        .replace('__DEVL__', dev_labels).replace('__DEVC__', dev_cost)
    )

    html = html.replace('__CHARTS_SCRIPT__', charts_js)

    # --- Seisa (精査) section ---
    target_cpa = TARGET_CPA_MAP.get(report_name, 0)
    seisa_threshold = target_cpa * 1.3
    html = html.replace('__SEISA_TGT__', fmt_yen(target_cpa))
    html = html.replace('__SEISA_THR__', fmt_yen(seisa_threshold))

    if target_cpa > 0:
        seisa = find_seisa_targets(df, target_cpa)
        seisa_html_parts = []
        level_labels = {'campaign': '🎯 キャンペーン', 'adgroup': '📁 アドグループ', 'ad': '📝 アド'}
        level_colors = {'campaign': '#991b1b', 'adgroup': '#9a3412', 'ad': '#92400e'}

        for level in ['campaign', 'adgroup', 'ad']:
            items = seisa[level]
            lbl = level_labels[level]
            lc = level_colors[level]
            count = len(items)

            if count == 0:
                seisa_html_parts.append(f'<div style="margin-bottom:16px;"><h3 style="font-size:14px;margin-bottom:6px;">{lbl} <span style="color:#16a34a;font-size:12px;">✅ 精査対象なし</span></h3></div>')
                continue

            rows = ''
            for r in items[:15]:
                nm = str(r['name'])
                if len(nm) > 65:
                    nm = nm[:62] + '...'
                cpa_display = fmt_yen(r['cpa']) if r['cpa'] < 900000 else 'CV無し'
                ratio_display = f'{r["ratio"]:.0f}%' if r['cpa'] < 900000 else '-'
                severity = '🔴' if r['ratio'] >= 200 or r['cpa'] >= 900000 else '🟡'
                rows += f'''<tr style="background:#fff9f9;">
                  <td>{severity}</td>
                  <td title="{r['name']}" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;">{nm}</td>
                  <td class="num">{fmt_yen(r['cost'])}</td>
                  <td class="num">{r['days']}日</td>
                  <td class="num">{fmt_num(r['click'])}</td>
                  <td class="num">{fmt_num(r['cv'], 0)}</td>
                  <td class="num" style="color:#dc2626;font-weight:600;">{cpa_display}</td>
                  <td class="num" style="color:#dc2626;font-weight:600;">{ratio_display}</td>
                  <td class="num">{fmt_pct(r['cvr'])}</td>
                </tr>'''

            seisa_html_parts.append(f'''<div style="margin-bottom:20px;">
              <h3 style="font-size:14px;margin-bottom:8px;">{lbl} <span style="background:#fee2e2;color:#dc2626;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600;">{count}件</span></h3>
              <div class="table-wrap"><table>
                <thead><tr><th></th><th>名前</th><th>費用</th><th>配信日数</th><th>Click</th><th>CV</th><th>CPA</th><th>目標比</th><th>CVR</th></tr></thead>
                <tbody>{rows}</tbody>
              </table></div>
            </div>''')

        seisa_total = sum(len(seisa[l]) for l in ['campaign', 'adgroup', 'ad'])
        summary_text = f'<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:10px;padding:14px;margin-bottom:16px;font-size:13px;color:#991b1b;"><strong>精査対象 合計: キャンペーン{len(seisa["campaign"])}件 / アドグループ{len(seisa["adgroup"])}件 / アド{len(seisa["ad"])}件</strong> — 目標CPA {fmt_yen(target_cpa)} の130%（{fmt_yen(seisa_threshold)}）以上 × 配信10日以上</div>'
        seisa_html = summary_text + '\n'.join(seisa_html_parts)
    else:
        seisa_html = '<p style="color:#94a3b8;">目標CPAが未設定のため精査対象の抽出ができません。targets.csvに目標CPAを設定してください。</p>'

    html = html.replace('__SEISA_HTML__', seisa_html)

    return html

###############################################################################
# Main
###############################################################################
if __name__ == '__main__':
    print("Loading data...")
    all_dfs = load_all_data()

    print("\nNormalizing...")
    df = normalize_and_combine(all_dfs)
    print(f"Combined: {len(df)} rows")

    print("\nSplitting by product (YDN only)...")
    datasets = split_data(df)

    for key, (name, data, c1, c2) in datasets.items():
        n_ydn = len(data)
        print(f"\n=== {name}: {n_ydn} rows ===")
        if n_ydn == 0:
            print("  SKIP (no data)")
            continue
        html = generate_report(name, data, c1, c2)
        outfile = os.path.join(DATA_DIR, f'report_{key}.html')
        with open(outfile, 'w', encoding='utf-8') as f:
            f.write(html)
        print(f"  Saved: {outfile}")

    # Generate index page with all reports
    card_items = []
    color_map = {
        'airpay_yda': ('#e3f2fd','#1a73e8'), 'onpare_yda': ('#e8f5e9','#0f9d58'),
        'onpare_biyou_yda': ('#f3e5f5','#ab47bc'), 'locomoa_yda': ('#fce4ec','#db4437'),
        'airpay_gdn': ('#e0f7fa','#00acc1'), 'onpare_biyou_gdn': ('#fbe9e7','#ff7043'),
    }
    for key, (name, data, c1, c2) in datasets.items():
        if len(data) == 0:
            continue
        bg, fg = color_map.get(key, ('#f5f5f5','#424242'))
        media_tag = 'GDN' if 'gdn' in key else 'YDA'
        card_items.append(f'''<a href="report_{key}.html" class="card">
    <h2>{name}</h2>
    <p>{name} の広告パフォーマンス分析</p>
    <span class="badge" style="background:{bg};color:{fg};">{media_tag}</span>
  </a>''')
    cards_html = '\n  '.join(card_items)

    index_html = f'''<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<title>広告レポート一覧</title>
<style>
body{{font-family:'Segoe UI','Meiryo',sans-serif;background:#f8f9fa;margin:0;padding:40px;}}
h1{{text-align:center;font-size:26px;color:#202124;}}
p.sub{{text-align:center;color:#5f6368;margin-top:8px;}}
h3.group{{max-width:1100px;margin:32px auto 12px;font-size:16px;color:#5f6368;border-bottom:2px solid #e0e0e0;padding-bottom:6px;}}
.card-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px;max-width:1100px;margin:0 auto 24px;}}
.card{{background:#fff;border-radius:14px;padding:28px;box-shadow:0 2px 8px rgba(0,0,0,0.08);transition:transform .2s;text-decoration:none;color:#202124;}}
.card:hover{{transform:translateY(-3px);box-shadow:0 8px 20px rgba(0,0,0,0.12);}}
.card h2{{font-size:18px;margin-bottom:6px;}}
.card p{{color:#5f6368;font-size:13px;}}
.card .badge{{display:inline-block;padding:3px 10px;border-radius:10px;font-size:11px;font-weight:600;margin-top:10px;}}
</style></head><body>
<h1>Web広告 パフォーマンスレポート</h1>
<p class="sub">生成日: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 媒体: YDA / GDN</p>
<h3 class="group">YDA (Yahoo!ディスプレイ広告)</h3>
<div class="card-grid">
  {cards_html}
</div>
</body></html>'''
    idx_path = os.path.join(DATA_DIR, 'index.html')
    with open(idx_path, 'w', encoding='utf-8') as f:
        f.write(index_html)
    print(f"\nIndex page: {idx_path}")
    print("\nDone!")
