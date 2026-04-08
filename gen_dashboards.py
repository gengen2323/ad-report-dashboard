# -*- coding: utf-8 -*-
import pandas as pd
from datetime import timedelta
import os

DATA_DIR = os.path.dirname(os.path.abspath(__file__))

# Load
dfs = []
for f in ['260331_CAD_Airぺイ.csv', '260406_CAD_Airペイ.csv']:
    df = pd.read_csv(os.path.join(DATA_DIR, f), sep='\t', encoding='utf-16-le', low_memory=False)
    df['dataset'] = 'Airペイ'
    df['cv'] = pd.to_numeric(df.iloc[:, 82], errors='coerce').fillna(0)
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

adf = pd.concat(dfs, ignore_index=True)
for c in ['cost', 'impression', 'click']:
    adf[c] = pd.to_numeric(adf[c], errors='coerce').fillna(0)
adf['report_date'] = pd.to_datetime(adf['report_date'], errors='coerce')
adf = adf.dropna(subset=['report_date'])

ydn = adf[adf['media'].str.contains('YDN', na=False)]
gdn = adf[adf['media'].str.contains('GDN', na=False)]
swe_ydn = ydn[ydn['dataset'].isin(['SWE_RTG', 'SWE_NonRTG'])]
swe_gdn = gdn[gdn['dataset'].isin(['SWE_RTG', 'SWE_NonRTG'])]

projects = {
    'airpay_yda': ('Airペイ × YDA', '#2563eb', ydn[(ydn['dataset'] == 'Airペイ') & ~ydn['campaign_name'].str.contains('CASM|SBI|テミクス', case=False, na=False)], 1000000, 38461, 26),
    'onpare_yda': ('オンパレ通常 × YDA', '#16a34a', swe_ydn[swe_ydn['campaign_name'].str.contains('onpr', case=False, na=False) & ~swe_ydn['campaign_name'].str.contains('美容', na=False)], 400000, 2500, 160),
    'onpare_biyou_yda': ('オンパレ美容 × YDA', '#9333ea', swe_ydn[swe_ydn['campaign_name'].str.contains('onpr', case=False, na=False) & swe_ydn['campaign_name'].str.contains('美容', na=False)], 400000, 2500, 160),
    'onpare_biyou_gdn': ('オンパレ美容 × GDN', '#ea580c', swe_gdn[swe_gdn['campaign_name'].str.contains('onpr', case=False, na=False) & swe_gdn['campaign_name'].str.contains('美容', na=False)], 200000, 2500, 80),
    'locomoa_yda': ('ロコモア × YDA', '#dc2626', swe_ydn[swe_ydn['campaign_name'].str.contains('lcmr', case=False, na=False)], 3300000, 11000, 300),
}

def fy(v):
    return '\u00A5{:,.0f}'.format(v) if v and v > 0 else '-'

def fn(v):
    return '{:,.0f}'.format(v) if v else '0'

def fp(v):
    return '{:.2f}%'.format(v) if v else '0%'

def tr(s, n=50):
    s = str(s)
    return s[:n - 2] + '..' if len(s) > n else s

for key, (title, color, data, budget, tgt_cpa, tgt_cv) in projects.items():
    if len(data) == 0:
        continue
    today = data['report_date'].max()
    ms = today.replace(day=1)
    w3 = today - timedelta(days=21)
    dm = data[data['report_date'] >= ms]
    dw = data[data['report_date'] >= w3]
    days = max((today - ms).days + 1, 1)

    mc = dm['cost'].sum(); mcv = dm['cv'].sum(); mclk = dm['click'].sum()
    mcpa = mc / mcv if mcv > 0 else 0
    wc = dw['cost'].sum(); wcv = dw['cv'].sum(); wclk = dw['click'].sum(); wimp = dw['impression'].sum()
    wcpa = wc / wcv if wcv > 0 else 0
    wcvr = (wcv / wclk * 100) if wclk > 0 else 0
    wctr = (wclk / wimp * 100) if wimp > 0 else 0

    cost_pct = mc / budget * 100 if budget > 0 else 0
    cv_pct = mcv / tgt_cv * 100 if tgt_cv > 0 else 0
    cv_proj = mcv / days * 30
    cpa_diff = ((mcpa - tgt_cpa) / tgt_cpa * 100) if tgt_cpa > 0 and mcpa > 0 else 0

    cost_col = '#d97706' if cost_pct > days / 30 * 100 * 1.2 else '#16a34a'
    cv_col = '#16a34a' if cv_proj >= tgt_cv else '#dc2626'
    cpa_col = '#16a34a' if (mcpa > 0 and mcpa <= tgt_cpa) else '#dc2626' if mcpa > 0 else '#94a3b8'

    # Top campaigns
    camps = []
    for cn, g in sorted(dw.groupby('campaign_name'), key=lambda x: -x[1]['cost'].sum())[:5]:
        c = g['cost'].sum(); v = g['cv'].sum(); k = g['click'].sum()
        camps.append((cn, c, v, c / v if v > 0 else 0, (v / k * 100) if k > 0 else 0))

    medals = ['\\U0001F947', '\\U0001F948', '\\U0001F949', '4', '5']
    camp_rows = ''
    for i, (cn, c, v, cpa, cvr) in enumerate(camps):
        camp_rows += '<tr><td>{}</td><td title="{}">{}</td><td class="n">{}</td><td class="n">{}</td><td class="n">{}</td><td class="n">{}</td></tr>'.format(
            medals[i], cn, tr(cn, 55), fy(c), fn(v), fy(cpa), fp(cvr))

    ags = []
    for agn, g in sorted(dw.groupby('adgroup_name'), key=lambda x: -x[1]['cost'].sum())[:5]:
        if pd.isna(agn):
            continue
        c = g['cost'].sum(); v = g['cv'].sum(); k = g['click'].sum()
        ags.append((agn, c, v, c / v if v > 0 else 0, (v / k * 100) if k > 0 else 0))

    ag_rows = ''
    for i, (agn, c, v, cpa, cvr) in enumerate(ags[:5]):
        ag_rows += '<tr><td>{}</td><td title="{}">{}</td><td class="n">{}</td><td class="n">{}</td><td class="n">{}</td><td class="n">{}</td></tr>'.format(
            medals[i], agn, tr(agn, 50), fy(c), fn(v), fy(cpa), fp(cvr))

    cost_proj = mc / days * 30
    cost_tag_bg = '#fef3c7' if cost_pct > days / 30 * 100 * 1.2 else '#dcfce7'
    cost_tag_fg = '#d97706' if cost_pct > days / 30 * 100 * 1.2 else '#16a34a'
    cv_tag_bg = '#dcfce7' if cv_proj >= tgt_cv else '#fee2e2'
    cv_tag_fg = '#16a34a' if cv_proj >= tgt_cv else '#dc2626'
    cpa_tag_bg = '#dcfce7' if mcpa <= tgt_cpa and mcpa > 0 else '#fee2e2'
    cpa_tag_fg = '#16a34a' if mcpa <= tgt_cpa and mcpa > 0 else '#dc2626'

    svg_r = 50
    svg_circ = 314  # 2*pi*50

    html = '''<!DOCTYPE html><html lang="ja"><head><meta charset="UTF-8">
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:'Segoe UI','Hiragino Kaku Gothic ProN','Meiryo',sans-serif;background:#f8fafc;color:#1e293b;width:1200px;padding:28px 32px;}
.hdr{display:flex;align-items:baseline;gap:14px;margin-bottom:18px;border-bottom:3px solid ''' + color + ''';padding-bottom:10px;}
.hdr h1{font-size:22px;color:''' + color + ''';}
.hdr .p{font-size:11px;color:#94a3b8;}
.row{display:flex;gap:14px;margin-bottom:14px;}
.gc{flex:1;background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:14px;text-align:center;box-shadow:0 1px 2px rgba(0,0,0,0.04);}
.gc .lb{font-size:9px;color:#94a3b8;text-transform:uppercase;letter-spacing:1px;}
.gc svg{width:90px;height:90px;margin:6px auto;display:block;}
.gc .bg{font-size:16px;font-weight:700;}
.gc .sm{font-size:10px;color:#94a3b8;}
.gc .tg{display:inline-block;padding:2px 7px;border-radius:4px;font-size:9px;font-weight:600;margin-top:3px;}
.kc{display:flex;flex-direction:column;gap:5px;flex:0.7;}
.ki{background:#fff;border:1px solid #e2e8f0;border-radius:8px;padding:8px;text-align:center;box-shadow:0 1px 2px rgba(0,0,0,0.04);}
.ki .lb{font-size:8px;color:#94a3b8;text-transform:uppercase;}
.ki .vl{font-size:15px;font-weight:700;margin-top:1px;}
.sec{margin-bottom:12px;}
.sec h2{font-size:13px;font-weight:700;margin-bottom:6px;}
table{width:100%;border-collapse:collapse;font-size:11px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;}
th{background:#f1f5f9;padding:6px 7px;text-align:left;font-weight:600;color:#64748b;font-size:9px;white-space:nowrap;}
td{padding:5px 7px;border-top:1px solid #f1f5f9;white-space:nowrap;}
td.n{text-align:right;font-variant-numeric:tabular-nums;}
tr:hover{background:#f0f9ff;}
.c2{display:flex;gap:14px;}.c2>div{flex:1;}
</style></head><body>

<div class="hdr">
  <h1>''' + title + '''</h1>
  <div class="p">''' + today.strftime('%Y-%m-%d') + ' | 4月 ' + str(days) + '''日経過 | CV=申込完了</div>
</div>

<div class="row">
  <div class="gc">
    <div class="lb">費用消化</div>
    <svg viewBox="0 0 120 120"><circle cx="60" cy="60" r="50" fill="none" stroke="#f1f5f9" stroke-width="9"/><circle cx="60" cy="60" r="50" fill="none" stroke="''' + cost_col + '''" stroke-width="9" stroke-dasharray="314" stroke-dashoffset="''' + str(int(314 - min(cost_pct, 100) / 100 * 314)) + '''" stroke-linecap="round" transform="rotate(-90 60 60)"/><text x="60" y="55" text-anchor="middle" font-size="17" font-weight="700" fill="''' + cost_col + '''">''' + '{:.0f}'.format(cost_pct) + '''%</text><text x="60" y="70" text-anchor="middle" font-size="8" fill="#94a3b8">消化率</text></svg>
    <div class="bg">''' + fy(mc) + '''</div>
    <div class="sm">目標 ''' + fy(budget) + '''/月</div>
    <div class="tg" style="background:''' + cost_tag_bg + ''';color:''' + cost_tag_fg + ''';">着地 ''' + fy(cost_proj) + '''</div>
  </div>
  <div class="gc">
    <div class="lb">CV達成</div>
    <svg viewBox="0 0 120 120"><circle cx="60" cy="60" r="50" fill="none" stroke="#f1f5f9" stroke-width="9"/><circle cx="60" cy="60" r="50" fill="none" stroke="''' + cv_col + '''" stroke-width="9" stroke-dasharray="314" stroke-dashoffset="''' + str(int(314 - min(cv_pct, 100) / 100 * 314)) + '''" stroke-linecap="round" transform="rotate(-90 60 60)"/><text x="60" y="55" text-anchor="middle" font-size="17" font-weight="700" fill="''' + cv_col + '''">''' + '{:.0f}'.format(cv_pct) + '''%</text><text x="60" y="70" text-anchor="middle" font-size="8" fill="#94a3b8">達成率</text></svg>
    <div class="bg">''' + fn(mcv) + '''件</div>
    <div class="sm">目標 ''' + fn(tgt_cv) + '''件/月</div>
    <div class="tg" style="background:''' + cv_tag_bg + ''';color:''' + cv_tag_fg + ''';">着地 ''' + '{:.0f}'.format(cv_proj) + '''件</div>
  </div>
  <div class="gc">
    <div class="lb">CPA</div>
    <svg viewBox="0 0 120 120"><circle cx="60" cy="60" r="50" fill="none" stroke="#f1f5f9" stroke-width="9"/><circle cx="60" cy="60" r="50" fill="none" stroke="''' + cpa_col + '''" stroke-width="9" stroke-dasharray="314" stroke-dashoffset="''' + str(int(314 - min(abs(mcpa / tgt_cpa * 100) if tgt_cpa > 0 and mcpa > 0 else 0, 150) / 150 * 314)) + '''" stroke-linecap="round" transform="rotate(-90 60 60)"/><text x="60" y="55" text-anchor="middle" font-size="13" font-weight="700" fill="''' + cpa_col + '''">''' + fy(mcpa) + '''</text><text x="60" y="70" text-anchor="middle" font-size="8" fill="#94a3b8">実績CPA</text></svg>
    <div class="sm">目標 ''' + fy(tgt_cpa) + '''</div>
    <div class="tg" style="background:''' + cpa_tag_bg + ''';color:''' + cpa_tag_fg + ''';">''' + '{:+.1f}'.format(cpa_diff) + '''%</div>
  </div>
  <div class="kc">
    <div class="ki"><div class="lb">3W費用</div><div class="vl" style="color:''' + color + ''';">''' + fy(wc) + '''</div></div>
    <div class="ki"><div class="lb">3W CV</div><div class="vl" style="color:#16a34a;">''' + fn(wcv) + '''</div></div>
    <div class="ki"><div class="lb">3W CPA</div><div class="vl">''' + fy(wcpa) + '''</div></div>
    <div class="ki"><div class="lb">CTR / CVR</div><div class="vl" style="font-size:12px;">''' + fp(wctr) + ' / ' + fp(wcvr) + '''</div></div>
  </div>
</div>

<div class="c2">
  <div class="sec">
    <h2>🎯 キャンペーン Top5（3W）</h2>
    <table><thead><tr><th>#</th><th>キャンペーン</th><th>費用</th><th>CV</th><th>CPA</th><th>CVR</th></tr></thead><tbody>''' + camp_rows + '''</tbody></table>
  </div>
  <div class="sec">
    <h2>📁 アドグループ Top5（3W）</h2>
    <table><thead><tr><th>#</th><th>アドグループ</th><th>費用</th><th>CV</th><th>CPA</th><th>CVR</th></tr></thead><tbody>''' + ag_rows + '''</tbody></table>
  </div>
</div>

</body></html>'''

    path = os.path.join(DATA_DIR, 'dash_{}.html'.format(key))
    with open(path, 'w', encoding='utf-8') as f:
        f.write(html)
    print('Saved {}'.format(path))

print('Done')
