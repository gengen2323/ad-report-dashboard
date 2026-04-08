# -*- coding: utf-8 -*-
"""Generate HTML slide decks for all projects with target vs actual gauges."""
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── Load & prepare data ───
def load_data():
    dfs = []
    for f in ['260331_CAD_Airぺイ.csv', '260406_CAD_Airペイ.csv']:
        df = pd.read_csv(os.path.join(DATA_DIR, f), sep='\t', encoding='utf-16-le', low_memory=False)
        df['dataset'] = 'Airペイ'
        df['cv'] = pd.to_numeric(df.iloc[:, 82], errors='coerce').fillna(0)  # 申込完了
        dfs.append(df)
    for f in ['260331_CAD_SWE_RTG.csv', '260406_CAD_SWE_RTG.csv']:
        df = pd.read_csv(os.path.join(DATA_DIR, f), sep='\t', encoding='utf-16-le', low_memory=False)
        df['dataset'] = 'SWE_RTG'
        df['cv'] = pd.to_numeric(df['conversion'], errors='coerce').fillna(0)
        dfs.append(df)
    for f in ['260331_CAD_SWE_NonRTG.csv', '260406_CAD_SWE_NonRTG.csv']:
        df = pd.read_csv(os.path.join(DATA_DIR, f), sep='\t', encoding='utf-16-le', low_memory=False)
        df['dataset'] = 'SWE_NonRTG'
        df['cv'] = pd.to_numeric(df['conversion'], errors='coerce').fillna(0)
        dfs.append(df)
    all_df = pd.concat(dfs, ignore_index=True)
    for c in ['cost','impression','click']:
        all_df[c] = pd.to_numeric(all_df[c], errors='coerce').fillna(0)
    all_df['report_date'] = pd.to_datetime(all_df['report_date'], errors='coerce')
    all_df = all_df.dropna(subset=['report_date'])
    return all_df

def split_projects(df):
    ydn = df[df['media'].str.contains('YDN', na=False)]
    gdn = df[df['media'].str.contains('GDN', na=False)]
    swe_ydn = ydn[ydn['dataset'].isin(['SWE_RTG','SWE_NonRTG'])]
    swe_gdn = gdn[gdn['dataset'].isin(['SWE_RTG','SWE_NonRTG'])]
    return {
        'airpay_yda': ('Airペイ × YDA', '#2563eb', '#1d4ed8',
            ydn[(ydn['dataset']=='Airペイ') & ~ydn['campaign_name'].str.contains('CASM|SBI|テミクス', case=False, na=False)]),
        'onpare_yda': ('オンパレ通常 × YDA', '#16a34a', '#15803d',
            swe_ydn[swe_ydn['campaign_name'].str.contains('onpr', case=False, na=False) & ~swe_ydn['campaign_name'].str.contains('美容', na=False)]),
        'onpare_biyou_yda': ('オンパレ美容 × YDA', '#9333ea', '#7e22ce',
            swe_ydn[swe_ydn['campaign_name'].str.contains('onpr', case=False, na=False) & swe_ydn['campaign_name'].str.contains('美容', na=False)]),
        'onpare_biyou_gdn': ('オンパレ美容 × GDN', '#ea580c', '#c2410c',
            swe_gdn[swe_gdn['campaign_name'].str.contains('onpr', case=False, na=False) & swe_gdn['campaign_name'].str.contains('美容', na=False)]),
        'locomoa_yda': ('ロコモア × YDA', '#dc2626', '#b91c1c',
            swe_ydn[swe_ydn['campaign_name'].str.contains('lcmr', case=False, na=False)]),
    }

def load_targets():
    t = pd.read_csv(os.path.join(DATA_DIR, 'targets.csv'), encoding='utf-8')
    targets = {}
    for _, r in t.iterrows():
        key = (r['案件'], r['媒体'])
        targets[key] = {'budget': r['月間目標費用'], 'target_cpa': r['目標CPA'], 'target_cv': r['目標CV']}
    return targets

TARGET_MAP = {
    'airpay_yda': ('Airペイ','YDA'),
    'onpare_yda': ('オンパレ通常','YDA'),
    'onpare_biyou_yda': ('オンパレ美容','YDA'),
    'onpare_biyou_gdn': ('オンパレ美容','GDN'),
    'locomoa_yda': ('ロコモア','YDA'),
}

# ─── Analysis helpers ───
def metrics(d):
    cost=d['cost'].sum(); imp=d['impression'].sum(); click=d['click'].sum(); cv=d['cv'].sum()
    return {'cost':cost,'imp':imp,'click':click,'cv':cv,
            'ctr':(click/imp*100) if imp>0 else 0, 'cpc':(cost/click) if click>0 else 0,
            'cpa':(cost/cv) if cv>0 else 0, 'cvr':(cv/click*100) if click>0 else 0,
            'cpm':(cost/imp*1000) if imp>0 else 0}

def top_items(d, col, n=5):
    res = []
    for name, g in d.groupby(col):
        if pd.isna(name) or str(name).strip()=='': continue
        m = metrics(g)
        m['name'] = name
        m['img'] = g['image_url'].dropna().iloc[0] if 'image_url' in g.columns and g['image_url'].notna().any() else ''
        res.append(m)
    res.sort(key=lambda x: -x['cost'])
    return res[:n]

def quadrant(d):
    items = top_items(d, 'ad_name', 100)
    if not items: return {'q1':[],'q2':[],'q3':[],'q4':[],'med_cr':0,'med_cpa':0}
    cdf = pd.DataFrame(items)
    total = cdf['cost'].sum()
    if total == 0: return {'q1':[],'q2':[],'q3':[],'q4':[],'med_cr':0,'med_cpa':0}
    cdf['cr'] = cdf['cost']/total*100
    med_cr = cdf['cr'].median()
    cv_items = cdf[cdf['cv']>0]
    med_cpa = cv_items['cpa'].median() if len(cv_items)>0 else 99999
    q1 = cdf[(cdf['cr']>=med_cr)&(cdf['cv']>0)&(cdf['cpa']<=med_cpa)].sort_values('cost',ascending=False).head(3).to_dict('records')
    q2 = cdf[(cdf['cr']>=med_cr)&((cdf['cv']==0)|(cdf['cpa']>med_cpa))].sort_values('cost',ascending=False).head(3).to_dict('records')
    q3 = cdf[(cdf['cr']<med_cr)&(cdf['cv']>0)&(cdf['cpa']<=med_cpa)].sort_values('cost',ascending=False).head(3).to_dict('records')
    q4 = cdf[(cdf['cr']<med_cr)&((cdf['cv']==0)|(cdf['cpa']>med_cpa))].sort_values('cost',ascending=False).head(3).to_dict('records')
    n1=len(cdf[(cdf['cr']>=med_cr)&(cdf['cv']>0)&(cdf['cpa']<=med_cpa)])
    n2=len(cdf[(cdf['cr']>=med_cr)&((cdf['cv']==0)|(cdf['cpa']>med_cpa))])
    n3=len(cdf[(cdf['cr']<med_cr)&(cdf['cv']>0)&(cdf['cpa']<=med_cpa)])
    n4=len(cdf[(cdf['cr']<med_cr)&((cdf['cv']==0)|(cdf['cpa']>med_cpa))])
    return {'q1':q1,'q2':q2,'q3':q3,'q4':q4,'med_cr':med_cr,'med_cpa':med_cpa,'n1':n1,'n2':n2,'n3':n3,'n4':n4}

# ─── Formatting helpers ───
def fy(v): return f'¥{v:,.0f}' if v and v>0 else '-'
def fn(v,d=0): return f'{v:,.{d}f}' if v else '-'
def fp(v): return f'{v:.2f}%' if v else '-'
def trunc(s,n=50): s=str(s); return s[:n-3]+'...' if len(s)>n else s

# ─── Gauge SVG helper ───
def gauge_offset(pct):
    """SVG circle dashoffset for a percentage (0-100). Full circle=440."""
    return int(440 - (min(pct, 100) / 100 * 440))

def gauge_color(val, target, lower_better=False):
    if lower_better:
        return '#16a34a' if val <= target else '#dc2626' if val > target*1.1 else '#d97706'
    else:
        return '#16a34a' if val >= target else '#dc2626' if val < target*0.5 else '#d97706'

# ─── HTML slide builder ───
def build_slide(key, title, c1, c2, data, target, all_data):
    if len(data)==0:
        return None

    today = data['report_date'].max()
    w3 = today - timedelta(days=21)
    m3 = today - timedelta(days=90)
    # Current month data (for target tracking)
    month_start = today.replace(day=1)
    d_month = data[data['report_date'] >= month_start]
    days_elapsed = (today - month_start).days + 1
    days_in_month = 30

    d3m = data[data['report_date'] >= m3]
    d3w = data[data['report_date'] >= w3]

    m_3m = metrics(d3m)
    m_3w = metrics(d3w)
    m_mo = metrics(d_month)

    # Target calcs
    budget = target['budget']
    tgt_cpa = target['target_cpa']
    tgt_cv = target['target_cv']
    cost_pct = (m_mo['cost']/budget*100) if budget>0 else 0
    cv_pct = (m_mo['cv']/tgt_cv*100) if tgt_cv>0 else 0
    cv_proj = m_mo['cv'] / days_elapsed * days_in_month if days_elapsed>0 else 0
    cpa_actual = m_mo['cpa']
    cpa_diff = ((cpa_actual - tgt_cpa)/tgt_cpa*100) if tgt_cpa>0 and cpa_actual>0 else 0

    cost_color = '#d97706' if cost_pct > days_elapsed/days_in_month*100*1.2 else '#16a34a'
    cv_color = gauge_color(m_mo['cv'], tgt_cv * days_elapsed/days_in_month)
    cpa_color = gauge_color(cpa_actual, tgt_cpa, lower_better=True)

    cost_badge_bg = '#fef3c7' if cost_pct > days_elapsed/days_in_month*100*1.2 else '#dcfce7'
    cost_badge_fg = '#d97706' if cost_pct > days_elapsed/days_in_month*100*1.2 else '#16a34a'
    cv_badge_bg = '#dcfce7' if cv_proj >= tgt_cv else '#fee2e2'
    cv_badge_fg = '#16a34a' if cv_proj >= tgt_cv else '#dc2626'
    cpa_badge_bg = '#dcfce7' if cpa_actual <= tgt_cpa else '#fee2e2'
    cpa_badge_fg = '#16a34a' if cpa_actual <= tgt_cpa else '#dc2626'

    # Top campaigns & adgroups
    camps = top_items(d3w, 'campaign_name', 5)
    ags = top_items(d3w, 'adgroup_name', 5)
    q = quadrant(d3w)

    # Campaign rows
    camp_rows = ''
    for i, c in enumerate(camps, 1):
        rc = ['rank-1','rank-2','rank-3','rank-n','rank-n'][i-1]
        camp_rows += f'<tr><td><span class="rank {rc}">{i}</span></td><td title="{c["name"]}">{trunc(c["name"],60)}</td>'
        camp_rows += f'<td class="num">{fy(c["cost"])}</td><td class="num">{fn(c["click"])}</td>'
        camp_rows += f'<td class="num">{fn(c["cv"])}</td><td class="num">{fy(c["cpa"])}</td><td class="num">{fp(c["cvr"])}</td></tr>'

    ag_rows = ''
    for i, a in enumerate(ags, 1):
        rc = ['rank-1','rank-2','rank-3','rank-n','rank-n'][i-1]
        ag_rows += f'<tr><td><span class="rank {rc}">{i}</span></td><td title="{a["name"]}">{trunc(a["name"],55)}</td>'
        ag_rows += f'<td class="num">{fy(a["cost"])}</td><td class="num">{fn(a["click"])}</td>'
        ag_rows += f'<td class="num">{fn(a["cv"])}</td><td class="num">{fy(a["cpa"])}</td><td class="num">{fp(a["cvr"])}</td></tr>'

    # Quadrant items
    def q_items(items, color):
        html = ''
        for r in items:
            img = str(r.get('img',''))
            img_tag = f'<img src="{img}" style="width:50px;height:32px;object-fit:cover;border-radius:4px;" onerror="this.style.display=\'none\'">' if img and img not in ('','nan') else ''
            html += f'''<div style="display:flex;gap:8px;align-items:center;background:#f8fafc;border-radius:8px;padding:7px;">
              {img_tag}<div style="flex:1;min-width:0;"><div style="font-size:11px;color:#1e293b;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">{trunc(r["name"],45)}</div>
              <div style="font-size:10px;color:{color};">{fy(r["cost"])} / CV{fn(r["cv"])} / CPA {fy(r["cpa"])} / CVR {fp(r["cvr"])}</div></div></div>'''
        return html

    # Build cost/CV/CPA action text
    cost_pace = m_mo['cost'] / days_elapsed * days_in_month if days_elapsed > 0 else 0
    cost_action = f'{days_elapsed}日で{cost_pct:.0f}%消化。月末着地¥{cost_pace:,.0f}見込。' + ('入札抑制検討。' if cost_pct > days_elapsed/days_in_month*100*1.2 else '良好ペース。')
    cv_action = f'日平均{m_mo["cv"]/days_elapsed:.1f}件。着地{cv_proj:.0f}件見込。' + (f'目標{tgt_cv}件に対して達成圏内。' if cv_proj >= tgt_cv else f'目標{tgt_cv}件に未達見込。CV効率改善が必要。')
    if cpa_actual > 0:
        cpa_action = f'実績CPA {fy(cpa_actual)}は目標{fy(tgt_cpa)}比{cpa_diff:+.1f}%。' + ('目標内で推移。' if cpa_actual <= tgt_cpa else 'CPA高騰中。低効率キャンペーンの見直しが必要。')
    else:
        cpa_action = 'CV未発生のためCPA算出不可。配信量とターゲティングを見直し。'

    # --- Assemble HTML ---
    html = f'''<!DOCTYPE html>
<html lang="ja"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title} レポート</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:'Segoe UI','Hiragino Kaku Gothic ProN','Meiryo',sans-serif;background:#f8fafc;color:#1e293b;overflow:hidden;height:100vh;}}
.slide-container{{height:100vh;overflow:hidden;position:relative;}}
.slide{{height:100vh;width:100%;display:none;flex-direction:column;justify-content:center;padding:40px 56px;position:relative;overflow:hidden;}}
.slide.active{{display:flex;}}
.nav-bar{{position:fixed;bottom:0;left:0;right:0;z-index:100;background:rgba(255,255,255,0.95);backdrop-filter:blur(12px);border-top:1px solid #e2e8f0;padding:10px 32px;display:flex;align-items:center;justify-content:space-between;}}
.nav-dots{{display:flex;gap:8px;}}
.nav-dot{{width:10px;height:10px;border-radius:50%;background:#cbd5e1;cursor:pointer;transition:all .3s;}}
.nav-dot.active{{background:{c1};width:28px;border-radius:5px;}}
.nav-btn{{background:#e2e8f0;border:none;color:#475569;width:40px;height:40px;border-radius:10px;cursor:pointer;font-size:18px;transition:all .2s;}}
.nav-btn:hover{{background:{c1};color:#fff;}}
.page-num{{color:#94a3b8;font-size:13px;}}
.slide-cover{{background:linear-gradient(135deg,#eff6ff,#f8fafc 50%,#f0f9ff);justify-content:center;align-items:center;text-align:center;}}
.slide-cover h1{{font-size:48px;font-weight:700;background:linear-gradient(135deg,{c1},{c2});-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:12px;}}
.slide-cover .sub{{font-size:20px;color:#64748b;margin-bottom:32px;}}
.slide-cover .meta{{font-size:14px;color:#94a3b8;display:flex;gap:24px;justify-content:center;}}
.stitle{{font-size:24px;font-weight:700;margin-bottom:20px;color:#0f172a;display:flex;align-items:center;gap:10px;}}
.badge{{display:inline-block;padding:3px 10px;border-radius:8px;font-size:11px;font-weight:600;}}
.kpi-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:16px 0;}}
.kpi{{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:22px 18px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.04);}}
.kpi .label{{font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;}}
.kpi .val{{font-size:28px;font-weight:700;color:#1e293b;}}
.kpi .val.blue{{color:{c1};}}
.kpi .val.green{{color:#16a34a;}}
.kpi .val.amber{{color:#d97706;}}
.kpi .val.red{{color:#dc2626;}}
.tbl-wrap{{overflow:auto;border-radius:10px;border:1px solid #e2e8f0;box-shadow:0 1px 3px rgba(0,0,0,0.04);}}
table{{width:100%;border-collapse:collapse;font-size:12px;background:#fff;}}
th{{background:#f1f5f9;padding:10px 12px;text-align:left;font-weight:600;color:#64748b;font-size:11px;text-transform:uppercase;white-space:nowrap;}}
td{{padding:9px 12px;border-top:1px solid #f1f5f9;white-space:nowrap;color:#334155;}}
td.num{{text-align:right;font-variant-numeric:tabular-nums;}}
tr:hover{{background:#f0f9ff;}}
.rank{{display:inline-flex;align-items:center;justify-content:center;width:24px;height:24px;border-radius:6px;font-weight:700;font-size:11px;}}
.rank-1{{background:#fef9c3;color:#a16207;}} .rank-2{{background:#f1f5f9;color:#64748b;}} .rank-3{{background:#fed7aa;color:#c2410c;}} .rank-n{{background:#f8fafc;color:#94a3b8;}}
.insight-grid{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px;margin-top:14px;}}
.insight{{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:20px;box-shadow:0 1px 3px rgba(0,0,0,0.04);}}
.insight .tag{{font-size:10px;text-transform:uppercase;letter-spacing:1px;margin-bottom:6px;font-weight:700;}}
.insight .tag.good{{color:#16a34a;}} .insight .tag.warn{{color:#d97706;}} .insight .tag.action{{color:#2563eb;}}
.insight p{{font-size:13px;line-height:1.6;color:#475569;}}
</style></head>
<body><div class="slide-container">

<!-- COVER -->
<div class="slide slide-cover active" data-idx="0">
  <h1>{title}</h1>
  <div class="sub">パフォーマンスレポート</div>
  <div class="meta"><span>📅 {d3w['report_date'].min().strftime('%Y-%m-%d')} 〜 {today.strftime('%Y-%m-%d')}（直近3週間）</span></div>
</div>

<!-- KPI -->
<div class="slide" data-idx="1">
  <div class="stitle">📊 直近3週間 KPIサマリ <span class="badge" style="background:#dbeafe;color:#2563eb;">CV = 申込完了</span></div>
  <div class="kpi-row">
    <div class="kpi"><div class="label">費用</div><div class="val blue">{fy(m_3w['cost'])}</div></div>
    <div class="kpi"><div class="label">IMP</div><div class="val">{fn(m_3w['imp'])}</div></div>
    <div class="kpi"><div class="label">Click</div><div class="val">{fn(m_3w['click'])}</div></div>
    <div class="kpi"><div class="label">CTR</div><div class="val amber">{fp(m_3w['ctr'])}</div></div>
  </div>
  <div class="kpi-row">
    <div class="kpi"><div class="label">CPC</div><div class="val">{fy(m_3w['cpc'])}</div></div>
    <div class="kpi"><div class="label">CV</div><div class="val green">{fn(m_3w['cv'])}</div></div>
    <div class="kpi"><div class="label">CPA</div><div class="val red">{fy(m_3w['cpa'])}</div></div>
    <div class="kpi"><div class="label">CVR</div><div class="val amber">{fp(m_3w['cvr'])}</div></div>
  </div>
  <div style="margin-top:12px;display:flex;gap:10px;">
    <span class="badge" style="background:#dbeafe;color:#2563eb;">3ヶ月: CV{fn(m_3m['cv'])} / CPA {fy(m_3m['cpa'])}</span>
    <span class="badge" style="background:#dcfce7;color:#16a34a;">3W: CPA {fy(m_3w['cpa'])} / CVR {fp(m_3w['cvr'])}</span>
  </div>
</div>

<!-- TARGET vs ACTUAL -->
<div class="slide" data-idx="2">
  <div class="stitle">🎯 目標 vs 実績（当月進捗） <span class="badge" style="background:#dbeafe;color:#2563eb;">{today.strftime('%m')}月 {days_elapsed}日経過</span></div>
  <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:18px;">
    <div style="background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:22px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;">費用</div>
      <div style="position:relative;width:140px;height:140px;margin:12px auto;">
        <svg viewBox="0 0 160 160" style="transform:rotate(-90deg);"><circle cx="80" cy="80" r="70" fill="none" stroke="#f1f5f9" stroke-width="12"/><circle cx="80" cy="80" r="70" fill="none" stroke="{cost_color}" stroke-width="12" stroke-dasharray="440" stroke-dashoffset="{gauge_offset(cost_pct)}" stroke-linecap="round"/></svg>
        <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);"><div style="font-size:24px;font-weight:700;color:{cost_color};">{cost_pct:.0f}%</div><div style="font-size:10px;color:#94a3b8;">消化率</div></div>
      </div>
      <div style="font-size:18px;font-weight:700;">{fy(m_mo['cost'])}</div>
      <div style="font-size:11px;color:#94a3b8;">目標: {fy(budget)} / 月</div>
      <div style="margin-top:6px;padding:3px 8px;border-radius:6px;font-size:10px;font-weight:600;background:{cost_badge_bg};color:{cost_badge_fg};display:inline-block;">着地¥{cost_pace:,.0f}</div>
    </div>
    <div style="background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:22px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;">CV</div>
      <div style="position:relative;width:140px;height:140px;margin:12px auto;">
        <svg viewBox="0 0 160 160" style="transform:rotate(-90deg);"><circle cx="80" cy="80" r="70" fill="none" stroke="#f1f5f9" stroke-width="12"/><circle cx="80" cy="80" r="70" fill="none" stroke="{cv_color}" stroke-width="12" stroke-dasharray="440" stroke-dashoffset="{gauge_offset(cv_pct)}" stroke-linecap="round"/></svg>
        <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);"><div style="font-size:24px;font-weight:700;color:{cv_color};">{cv_pct:.0f}%</div><div style="font-size:10px;color:#94a3b8;">達成率</div></div>
      </div>
      <div style="font-size:18px;font-weight:700;">{fn(m_mo['cv'])}件</div>
      <div style="font-size:11px;color:#94a3b8;">目標: {fn(tgt_cv)}件 / 月</div>
      <div style="margin-top:6px;padding:3px 8px;border-radius:6px;font-size:10px;font-weight:600;background:{cv_badge_bg};color:{cv_badge_fg};display:inline-block;">着地{cv_proj:.0f}件見込</div>
    </div>
    <div style="background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:22px;text-align:center;box-shadow:0 1px 3px rgba(0,0,0,0.04);">
      <div style="font-size:11px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;">CPA</div>
      <div style="position:relative;width:140px;height:140px;margin:12px auto;">
        <svg viewBox="0 0 160 160" style="transform:rotate(-90deg);"><circle cx="80" cy="80" r="70" fill="none" stroke="#f1f5f9" stroke-width="12"/><circle cx="80" cy="80" r="70" fill="none" stroke="{cpa_color}" stroke-width="12" stroke-dasharray="440" stroke-dashoffset="{gauge_offset(min(cpa_actual/tgt_cpa*100,200) if tgt_cpa>0 and cpa_actual>0 else 0)}" stroke-linecap="round"/></svg>
        <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);"><div style="font-size:20px;font-weight:700;color:{cpa_color};">{fy(cpa_actual)}</div><div style="font-size:10px;color:#94a3b8;">実績CPA</div></div>
      </div>
      <div style="font-size:14px;color:#94a3b8;">目標: <strong style="color:#1e293b;">{fy(tgt_cpa)}</strong></div>
      <div style="margin-top:6px;padding:3px 8px;border-radius:6px;font-size:10px;font-weight:600;background:{cpa_badge_bg};color:{cpa_badge_fg};display:inline-block;">目標比 {cpa_diff:+.1f}%</div>
    </div>
  </div>
  <div style="margin-top:14px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;">
    <div style="background:#eff6ff;border-radius:8px;padding:12px;font-size:11px;color:#1e40af;"><strong>💰 費用:</strong> {cost_action}</div>
    <div style="background:#f0fdf4;border-radius:8px;padding:12px;font-size:11px;color:#166534;"><strong>📈 CV:</strong> {cv_action}</div>
    <div style="background:#fef2f2;border-radius:8px;padding:12px;font-size:11px;color:#991b1b;"><strong>⚡ CPA:</strong> {cpa_action}</div>
  </div>
</div>

<!-- CAMPAIGN -->
<div class="slide" data-idx="3">
  <div class="stitle">🎯 キャンペーン別 Top5</div>
  <div class="tbl-wrap"><table>
    <thead><tr><th>#</th><th>キャンペーン</th><th style="text-align:right">費用</th><th style="text-align:right">Click</th><th style="text-align:right">CV</th><th style="text-align:right">CPA</th><th style="text-align:right">CVR</th></tr></thead>
    <tbody>{camp_rows}</tbody>
  </table></div>
</div>

<!-- ADGROUP -->
<div class="slide" data-idx="4">
  <div class="stitle">📁 アドグループ別 Top5</div>
  <div class="tbl-wrap"><table>
    <thead><tr><th>#</th><th>アドグループ</th><th style="text-align:right">費用</th><th style="text-align:right">Click</th><th style="text-align:right">CV</th><th style="text-align:right">CPA</th><th style="text-align:right">CVR</th></tr></thead>
    <tbody>{ag_rows}</tbody>
  </table></div>
</div>

<!-- 4-QUADRANT -->
<div class="slide" data-idx="5">
  <div class="stitle">🎨 クリエイティブ 4象限分析 <span class="badge" style="background:#dbeafe;color:#2563eb;">中央値CPA {fy(q['med_cpa'])}</span></div>
  <div style="display:grid;grid-template-columns:1fr 1fr;grid-template-rows:1fr 1fr;gap:10px;flex:1;min-height:0;">
    <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:12px;padding:14px;overflow-y:auto;">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;"><span style="font-size:18px;">🏆</span><div><div style="font-size:13px;font-weight:700;color:#16a34a;">Winner</div><div style="font-size:9px;color:#64748b;">高コスト × 低CPA</div></div><span class="badge" style="background:#dcfce7;color:#16a34a;margin-left:auto;">{q.get('n1',0)}件</span></div>
      {q_items(q['q1'], '#16a34a')}
    </div>
    <div style="background:#fffbeb;border:1px solid #fde68a;border-radius:12px;padding:14px;overflow-y:auto;">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;"><span style="font-size:18px;">⚠️</span><div><div style="font-size:13px;font-weight:700;color:#d97706;">要改善</div><div style="font-size:9px;color:#64748b;">高コスト × 高CPA</div></div><span class="badge" style="background:#fef3c7;color:#d97706;margin-left:auto;">{q.get('n2',0)}件</span></div>
      {q_items(q['q2'], '#d97706')}
    </div>
    <div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:14px;overflow-y:auto;">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;"><span style="font-size:18px;">💎</span><div><div style="font-size:13px;font-weight:700;color:#2563eb;">隠れ優良</div><div style="font-size:9px;color:#64748b;">低コスト × 低CPA</div></div><span class="badge" style="background:#dbeafe;color:#2563eb;margin-left:auto;">{q.get('n3',0)}件</span></div>
      {q_items(q['q3'], '#2563eb')}
    </div>
    <div style="background:#fef2f2;border:1px solid #fecaca;border-radius:12px;padding:14px;overflow-y:auto;">
      <div style="display:flex;align-items:center;gap:6px;margin-bottom:8px;"><span style="font-size:18px;">✂️</span><div><div style="font-size:13px;font-weight:700;color:#dc2626;">停止検討</div><div style="font-size:9px;color:#64748b;">低コスト × CV無/高CPA</div></div><span class="badge" style="background:#fee2e2;color:#dc2626;margin-left:auto;">{q.get('n4',0)}件</span></div>
      {q_items(q['q4'], '#dc2626')}
    </div>
  </div>
</div>

</div>
<div class="nav-bar"><div class="nav-dots" id="dots"></div><div class="page-num" id="pn">1/6</div><div style="display:flex;gap:10px;"><button class="nav-btn" onclick="go(-1)">&#8592;</button><button class="nav-btn" onclick="go(1)">&#8594;</button></div></div>
<script>
const ss=document.querySelectorAll('.slide');let cur=0;
function show(n){{cur=Math.max(0,Math.min(n,ss.length-1));ss.forEach(s=>s.classList.remove('active'));ss[cur].classList.add('active');document.querySelectorAll('.nav-dot').forEach((d,i)=>d.classList.toggle('active',i===cur));document.getElementById('pn').textContent=(cur+1)+'/'+ss.length;}}
function go(d){{show(cur+d);}}
const de=document.getElementById('dots');ss.forEach((_,i)=>{{const d=document.createElement('div');d.className='nav-dot'+(i===0?' active':'');d.onclick=()=>show(i);de.appendChild(d);}});
document.addEventListener('keydown',e=>{{if(e.key==='ArrowRight'||e.key===' ')go(1);if(e.key==='ArrowLeft')go(-1);}});
</script></body></html>'''
    return html


# ─── Main ───
if __name__ == '__main__':
    print("Loading data...")
    df = load_data()
    print(f"Total: {len(df)} rows")

    targets = load_targets()
    projects = split_projects(df)

    for key, (title, c1, c2, data) in projects.items():
        n = len(data)
        print(f"\n=== {title}: {n} rows ===")
        if n == 0:
            print("  SKIP")
            continue
        tgt_key = TARGET_MAP.get(key)
        target = targets.get(tgt_key, {'budget':0,'target_cpa':0,'target_cv':0})

        html = build_slide(key, title, c1, c2, data, target, df)
        if html:
            path = os.path.join(DATA_DIR, f'slide_{key}.html')
            with open(path, 'w', encoding='utf-8') as f:
                f.write(html)
            print(f"  Saved: {path}")

    print("\nDone!")
