
# Creates two figures showing how UKIP and Reform UK link NHS/public services
# to immigration in their public communications:
#
#   immigration_NHS_linkage_YouTube.png
#       NHS–immigration linkage rates in UKIP and Reform UK YouTube transcripts
#       over time, using a sliding-window co-occurrence + causal-connector approach.
#
#   immigration_NHS_linkage_press_releases.png
#       The same linkage measured in UKIP press releases (2010–2024),
#       using a sentence-level annotation approach.
#
# Data sources:
#   - UKIP YouTube transcripts:   ../data/ukip_videos_with_meta.csv
#   - Reform UK YouTube transcripts: ../data/reform_videos_with_meta.csv
#   - UKIP press releases:        ../data/ukip_press_releases.parquet

import re
import math
import warnings
from collections import defaultdict, namedtuple

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import spacy

warnings.filterwarnings("ignore")

OUTPUT_DIR = '../final_output_for_article/'

import os
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# ── Lexicons ──────────────────────────────────────────────────────────────────

PUBLIC_SERVICES_TERMS = [
    'nhs', 'national health', 'national healthcare', 'healthcare', 'health service',
    'national health service', 'public health', 'public healthcare', 'healthcare system',
    'gp', 'general practitioner', 'family doctor', 'doctor shortage', 'nurse shortage',
    'hospital', 'hospitals', 'accident and emergency', 'a&e', 'waiting list', 'waiting lists',
    'waiting time', 'waiting times'
]

IMMIGRATION_TERMS = [
    'immigrant', 'immigrants', 'immigration', 'migrant', 'migrants', 'migrant workers',
    'illegal immigration', 'illegal immigrant', 'illegal immigrants',
    'asylum seeker', 'asylum seekers', 'refugee', 'refugees',
    'border control', 'border security', 'channel crossing', 'channel crossings',
    'small boat', 'small boats', 'deportation', 'migrant crisis',
    'foreigner', 'foreigners', 'overcrowding', 'overcrowded', 'population growth',
    'too many people', 'too much immigration', 'mass immigration'
]


# ── Connector patterns (shared by both analysis approaches) ───────────────────

CONNECTORS_ANY = [
    r"because", r"due to", r"thanks to", r"as a result of",
    r"caus(?:e|ed|es|ing)\b", r"driv(?:e|en|es|ing)\b",
    r"lead(?:s|ing)? to", r"result(?:s|ing)? in",
    r"put(?:s|ting)? pressure on", r"put(?:s|ting)? strain on",
    r"push(?:es|ing)? .* over the edge",
    r"overwhelm(?:s|ing)?", r"overburden(?:s|ing)?",
    r"contribut(?:e|es|ed|ing)\b to",
    r"linked to", r"responsible for",
    r"impact(?:s|ed|ing)? (?:on )?(?:the )?",
    r"harm(?:s|ed|ing)? (?:the )?",
    r"damage(?:s|ed|ing)? (?:the )?",
    r"undermine(?:s|d|ing)? (?:the )?",
    r"crowd(?:s|ed|ing)? (?:the )?",
    r"increas(?:e|es|ed|ing)\b (?:the )?(?:burden|pressure|strain|demand)",
    r"worsen(?:s|ed|ing)? (?:the )?(?:crisis|situation|problems?)",
    r"exacerbat(?:e|es|ed|ing)\b (?:the )?(?:crisis|situation|problems?)",
    r"add(?:s|ed|ing)? to (?:the )?(?:burden|pressure|strain|demand)",
    r"fuel(?:s|ed|ing)? (?:the|our )?(?:crisis|problems?)",
    r"stretch(?:es|ed|ing)? (?:the|our )?",
    r"tax(?:es|ed|ing)? (?:the|our )?",
    r"overload(?:s|ed|ing)? (?:the|our )?",
    r"overstretch(?:es|ed|ing)? (?:the|our )?",
    r"affect(?:s|ed|ing)? (?:the|our )?",
    r"swamp(?:s|ed|ing)? (?:the|our )?",
    r"clog(?:s|ged|ging)? (?:the|our )?",
    r"harm(?:s|ed|ing)? (?:the|our )?",
    r"damage(?:s|ed|ing)? (?:the|our )?",
    r"undermine(?:s|d|ing)? (?:the|our )?",
    r"crowd(?:s|ed|ing)? (?:our )?",
    r"increas(?:e|es|ed|ing)\b (?:our )?(?:burden|pressure|strain|demand)",
    r"worsen(?:s|ed|ing)? (?:our )?(?:crisis|situation|problems?)",
    r"exacerbat(?:e|es|ed|ing)\b (?:our )?(?:crisis|situation|problems?)",
    r"add(?:s|ed|ing)? to (?:our )?(?:burden|pressure|strain|demand)",
    r"fuel(?:s|ed|ing)? (?:our )?(?:crisis|problems?)",
    r"stretch(?:es|ed|ing)? (?:our )?",
    r"tax(?:es|ed|ing)? (?:our )?",
    r"overload(?:s|ed|ing)? (?:our )?",
    r"overstretch(?:es|ed|ing)? (?:our )?",
    r"affect(?:s|ed|ing)? (?:our )?",
    r"swamp(?:s|ed|ing)? (?:our )?",
    r"clog(?:s|ged|ging)? (?:our )?",
    r"rais(?:e|es|ed|ing)\b demand",
    r"make(?:s|ing|d)? .* (worse|collapse|break down)",
    r"keep(?:s|ing)? under strain",
    r"blame", r"is why .* (nhs|hospital|public service)",
    r"means .* (nhs|hospital|public service)"
]

CONNECTORS_EXPLICIT = [
    r"because", r"due to", r"thanks to",
    r"caus(?:e|ed|es|ing)\b", r"driv(?:e|en|es|ing)\b",
    r"lead(?:s|ing)? to", r"result(?:s|ing)? in",
    r"put(?:s|ting)? pressure on", r"put(?:s|ting)? strain on",
]


# ── Tokenisation & phrase matching ────────────────────────────────────────────

TOKEN_RE = re.compile(r"[a-z0-9]+(?:[-][a-z0-9]+)?")

def normalize(text): return re.sub(r"\s+", " ", text.lower()).strip()
def tokenize(text):  return TOKEN_RE.findall(text)

def prep_phrases(phrases):
    buckets = defaultdict(list)
    for p in phrases:
        toks = tuple(tokenize(p))
        if toks:
            buckets[toks[0]].append((toks, len(toks)))
    return buckets

PS_BUCKETS  = prep_phrases(PUBLIC_SERVICES_TERMS)
IMM_BUCKETS = prep_phrases(IMMIGRATION_TERMS)

def find_positions(tokens, buckets):
    hits = []
    n = len(tokens)
    for i, t in enumerate(tokens):
        if t not in buckets: continue
        for toks, L in buckets[t]:
            if i + L <= n and tuple(tokens[i:i+L]) == toks:
                hits.append((i, L))
    return np.array([h[0] for h in hits], dtype=int)

def connector_positions(tokens, patterns):
    text = " ".join(tokens)
    idxs = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            token_idx = text[:m.start()].count(" ")
            idxs.append(token_idx)
    return np.array(sorted(set(idxs)), dtype=int)


# ── Windowing linkage counter ─────────────────────────────────────────────────

def count_linkage_windows(tokens, window_size=80, stride=10, proximity=12,
                           mode="cooc+prox", use_explicit=False):
    n = len(tokens)
    if n == 0: return (0, 0)

    ppos  = find_positions(tokens, PS_BUCKETS)
    ipos  = find_positions(tokens, IMM_BUCKETS)
    conns = connector_positions(tokens, CONNECTORS_EXPLICIT if use_explicit else CONNECTORS_ANY)

    total = 0; link = 0
    i = 0
    while i < n:
        start = i; end = min(i + window_size, n); total += 1
        p_hits = ppos[(ppos >= start) & (ppos < end)]
        i_hits = ipos[(ipos >= start) & (ipos < end)]
        if len(p_hits) == 0 or len(i_hits) == 0:
            i += stride; continue
        if mode == "cooc":
            link += 1
        elif mode == "cooc+anyconn":
            c = conns[(conns >= start) & (conns < end)]
            if len(c): link += 1
        else:  # cooc+prox
            c = conns[(conns >= start) & (conns < end)]
            if len(c):
                if (len(p_hits) and np.min(np.abs(c[:, None] - p_hits[None, :])) <= proximity) or \
                   (len(i_hits) and np.min(np.abs(c[:, None] - i_hits[None, :])) <= proximity):
                    link += 1
        i += stride
    return (link, total)


def compute_rates(df, mode="cooc+prox", use_explicit=True,
                  window_size=80, stride=10, proximity=12):
    rows = []
    for r in df.itertuples(index=False):
        vid  = getattr(r, 'video_id')
        date = pd.to_datetime(getattr(r, 'date'))
        chan = getattr(r, 'channel', None) if 'channel' in df.columns else None
        toks = tokenize(normalize(getattr(r, 'text')))
        link, total = count_linkage_windows(
            toks, window_size, stride, proximity, mode=mode, use_explicit=use_explicit
        )
        rows.append({
            "video_id": vid, "date": date, "channel": chan,
            "n_tokens": len(toks), "n_windows": total,
            "link_windows": link,
            "rate": link / total if total else 0.0
        })
    m = pd.DataFrame(rows)
    m['month'] = m['date'].dt.to_period('M').dt.to_timestamp()
    ts = (m.groupby('month')
           .apply(lambda g: pd.Series({
               "windows": g['n_windows'].sum(),
               "links":   g['link_windows'].sum(),
               "rate":    (g['link_windows'].sum() / g['n_windows'].sum())
                          if g['n_windows'].sum() else 0.0,
               "n_videos": len(g),
               'party': g['party'].iloc[0] if 'party' in g.columns else None
           }))
           .reset_index())
    return m, ts


WindowHit = namedtuple("WindowHit", "start end text mode")

def sample_windows(row, mode="misses", k=10, window_size=80, stride=10,
                   proximity=12, use_explicit=True):
    toks = tokenize(normalize(row['text']))
    n = len(toks)
    epos  = find_positions(toks, PS_BUCKETS)
    nzpos = find_positions(toks, IMM_BUCKETS)
    conns = connector_positions(toks, CONNECTORS_EXPLICIT if use_explicit else CONNECTORS_ANY)

    def cooc(start, end):
        e = epos[(epos >= start) & (epos < end)]
        z = nzpos[(nzpos >= start) & (nzpos < end)]
        return len(e) > 0 and len(z) > 0

    def ok(start, end):
        if not cooc(start, end): return False
        c = conns[(conns >= start) & (conns < end)]
        if len(c) == 0: return False
        e = epos[(epos >= start) & (epos < end)]
        z = nzpos[(nzpos >= start) & (nzpos < end)]
        near_e = len(e) and np.min(np.abs(c[:, None] - e[None, :])) <= proximity
        near_z = len(z) and np.min(np.abs(c[:, None] - z[None, :])) <= proximity
        return bool(near_e or near_z)

    hits = []; misses = []
    i = 0
    while i < n:
        s = i; e = min(i + window_size, n)
        if cooc(s, e):
            (hits if ok(s, e) else misses).append((s, e))
        i += stride

    choose = hits if mode == "hits" else misses
    np.random.shuffle(choose)
    return ["..." + " ".join(toks[max(0, s-10):min(n, e+10)]) + "..."
            for s, e in choose[:k]]


def calculate_equations(df, window_size=50, stride=10, proximity=12, party=None):
    df = df.rename(columns={'transcript': 'text', 'upload_date': 'date'})
    df['date'] = pd.to_datetime(df['date'])

    _, ts_cooc   = compute_rates(df, mode="cooc",        use_explicit=False,
                                  window_size=window_size, stride=stride)
    _, ts_any    = compute_rates(df, mode="cooc+anyconn", use_explicit=False,
                                  window_size=window_size, stride=stride)
    _, ts_causal = compute_rates(df, mode="cooc+prox",   use_explicit=True,
                                  window_size=window_size, stride=stride, proximity=proximity)
    return ts_cooc, ts_any, ts_causal


# ── Sentence-level analysis (press releases) ──────────────────────────────────

nlp = spacy.load("en_core_web_sm")
if "parser" not in nlp.pipe_names and "senter" not in nlp.pipe_names:
    nlp.add_pipe("sentencizer")

NHS_TERMS   = r"\b(nhs|gp(?:s)?|a&e|accident and emergency|general practice|doctor(?:s)?|nurse(?:s)?|waiting (?:list|time)s?|hospital(?:s)?|primary care|surgery)\b"
IMMIG_TERMS = r"\b(immigration|immigrant(?:s)?|migrant(?:s)?|asylum seeker(?:s)?|asylum|refugee(?:s)?|small boats?|mass immigration|free movement|illegal immigration)\b"
CONNECT_EXPL = r"\b(because|caus(?:e|es|ing)|driv(?:e|es)|responsible for|to blame|blame(?:s|d)?)\b"
NEGATORS     = r"\b(not|no|never|hardly|rarely|scarcely|barely|without)\b"

CAUSE_VERBS = {"cause","drive","burden","strain","pressure","overwhelm",
               "increase","worsen","damage","undermine","crowd","impact","harm","break"}

re_nhs  = re.compile(NHS_TERMS,     re.I)
re_imm  = re.compile(IMMIG_TERMS,   re.I)
re_any  = re.compile("|".join(f"(?:{p})" for p in CONNECTORS_ANY), re.I)
re_expl = re.compile(CONNECT_EXPL,  re.I)
re_nega = re.compile(NEGATORS,      re.I)


def annotate_sentence(txt):
    return pd.Series({
        "has_nhs":  bool(re_nhs.search(txt)),
        "has_imm":  bool(re_imm.search(txt)),
        "has_any":  bool(re_any.search(txt)),
        "has_expl": bool(re_expl.search(txt)),
        "has_neg":  bool(re_nega.search(txt)),
    })


def directionality_pass(sent_span):
    def is_imm(w): return re_imm.search(w.text) is not None
    def is_nhs(w): return re_nhs.search(w.text) is not None
    for tok in sent_span:
        if tok.lemma_.lower() in CAUSE_VERBS and tok.pos_ in {"VERB", "AUX", "NOUN"}:
            subj_like = [c for c in tok.children
                         if c.dep_ in {"nsubj","nsubjpass","agent","compound","amod"}]
            if not any(is_imm(w) for w in subj_like):
                continue
            obj_like  = [c for c in tok.children
                         if c.dep_ in {"dobj","obj","attr","oprd"}]
            pobj_like = [gc for c in tok.children if c.dep_ == "prep"
                         for gc in c.children if gc.dep_ == "pobj"]
            if any(is_nhs(w) for w in obj_like + pobj_like) or \
               any(is_nhs(w) for w in tok.subtree):
                return True
    return False


def sentences_from_doc(doc_id, date, text):
    doc = nlp(text)
    rows = []
    for i, s in enumerate(doc.sents):
        s_txt = s.text.strip()
        if not s_txt:
            continue
        ann = annotate_sentence(s_txt)
        rows.append({
            "doc_id":   doc_id,
            "date":     pd.to_datetime(date),
            "sent_id":  i,
            "text":     s_txt,
            **ann,
            "dir_pass": directionality_pass(s),
        })
    return rows


def annotate_press_releases(press_df):
    all_rows = []
    for r in press_df.itertuples(index=False):
        all_rows.extend(sentences_from_doc(r.doc_id, r.date, r.text))
    sents = pd.DataFrame(all_rows)

    sents["imm_any"]     = sents["has_imm"] & sents["has_any"]  & ~sents["has_neg"]
    sents["imm_expl"]    = sents["has_imm"] & sents["has_expl"] & ~sents["has_neg"]
    sents["same_sent_co"] = sents["has_nhs"] & sents["has_imm"]

    sents = sents.sort_values(["doc_id", "sent_id"]).copy()
    sents["nhs_prev"] = sents.groupby("doc_id")["has_nhs"].shift(1).fillna(False)
    sents["nhs_next"] = sents.groupby("doc_id")["has_nhs"].shift(-1).fillna(False)
    sents["adj_any"]  = sents["imm_any"]  & (sents["nhs_prev"] | sents["nhs_next"])
    sents["adj_expl"] = sents["imm_expl"] & (sents["nhs_prev"] | sents["nhs_next"])

    gb = sents.groupby("doc_id")
    co_mention       = gb.apply(lambda g: g["same_sent_co"].any())
    any_causal_sent  = gb.apply(lambda g: (g["has_nhs"] & g["imm_any"]).any())
    expl_causal_sent = gb.apply(lambda g: (g["has_nhs"] & g["imm_expl"]).any())
    any_causal_adj   = gb["adj_any"].any()
    expl_causal_adj  = gb["adj_expl"].any()
    dir_pass_doc     = gb["dir_pass"].any()
    n_sents          = gb["sent_id"].count()

    sent_agg = pd.DataFrame({
        "doc_id":           co_mention.index,
        "co_mention":       co_mention.values,
        "any_causal_sent":  any_causal_sent.values,
        "any_causal_adj":   any_causal_adj.values,
        "expl_causal_sent": expl_causal_sent.values,
        "expl_causal_adj":  expl_causal_adj.values,
        "dir_pass":         dir_pass_doc.values,
        "n_sents":          n_sents.values,
    })

    doc_meta = sents[["doc_id", "date"]].drop_duplicates()
    docs = doc_meta.merge(sent_agg, on="doc_id", how="left")
    docs["any_causal"]  = docs["any_causal_sent"]  | docs["any_causal_adj"]
    docs["expl_causal"] = docs["expl_causal_sent"] | docs["expl_causal_adj"]
    return sents.reset_index(drop=True), docs


def docs_to_monthly_timeseries(docs):
    docs = docs.copy()
    docs["month"] = docs["date"].values.astype("datetime64[M]")

    def make_series(flag):
        g = docs.groupby("month").agg(windows=("doc_id", "count"), hits=(flag, "sum"))
        g["rate"] = (g["hits"] / g["windows"].replace(0, np.nan)).fillna(0.0)
        return g.reset_index()[["month", "rate", "windows"]]

    return make_series("co_mention"), make_series("any_causal"), make_series("expl_causal")


# ── Shared plotting function ──────────────────────────────────────────────────

def plot_two_parties(ukip, reform, title,
                     series=("Co-occurrence (NHS + Immigration)", "Explicit causal linkage"),
                     smooth_window=10, low_volume_threshold=400,
                     events=None, height=540, palette=None):
    valid = {
        "Co-occurrence (NHS + Immigration)": 0,
        "+ any connector": 1,
        "Explicit causal linkage": 2,
    }
    if palette is None:
        palette = {"UKIP": "#7B1FA2", "Reform UK": "#1B9E77"}

    dash = {
        "Co-occurrence (NHS + Immigration)": "dot",
        "+ any connector": "dash",
        "Explicit causal linkage": "solid",
    }

    party_data = {}
    if ukip   is not None: party_data["UKIP"]      = ukip
    if reform is not None: party_data["Reform UK"] = reform

    def _prep(ts, name):
        t = ts.sort_values("month").copy()
        t["rate_smooth"] = t["rate"].rolling(smooth_window, center=True, min_periods=1).mean()
        t["series"] = name
        return t

    frames = []
    for party, triple in party_data.items():
        for s in series:
            d = _prep(list(triple)[valid[s]], s)
            d["party"] = party
            frames.append(d)
    df = pd.concat(frames, ignore_index=True)

    fig = go.Figure()
    for party in party_data:
        for name in series:
            d = df[(df["party"] == party) & (df["series"] == name)]
            if d.empty: continue
            fig.add_trace(go.Scatter(
                x=d["month"], y=d["rate_smooth"],
                mode="lines",
                name=f"{party} — {name}",
                legendgroup=party,
                line=dict(width=4, dash=dash[name], color=palette.get(party)),
                hovertemplate=(
                    "<b>%{text}</b><br>Party: %{customdata[2]}<br>"
                    "Month: %{x|%b %Y}<br>Smoothed rate: %{y:.3%}<br>"
                    "Raw rate: %{customdata[0]:.3%}<br>"
                    "Windows: %{customdata[1]:,d}<extra></extra>"
                ),
                text=[name] * len(d),
                customdata=np.c_[d["rate"].values, d["windows"].values, d["party"].values],
            ))

    if df["month"].notna().any():
        full_index = pd.date_range(df["month"].min(), df["month"].max(), freq="MS")
        vol = df.groupby("month")["windows"].sum().reindex(full_index).fillna(0)
        for m, w in vol.items():
            if w < low_volume_threshold:
                fig.add_vrect(
                    x0=pd.to_datetime(m),
                    x1=(pd.to_datetime(m) + pd.offsets.MonthEnd(1)),
                    fillcolor="lightgray", opacity=0.12, line_width=0, layer="below",
                )

    if events:
        heights = [1.05, 1.05, 1.05]
        for i, ev in enumerate(events):
            dte = pd.to_datetime(ev["date"])
            fig.add_vline(x=dte, line_width=1.5, line_dash="dash", opacity=0.8)
            fig.add_annotation(
                x=dte, y=heights[i % len(heights)],
                xref="x", yref="paper",
                showarrow=False, text=ev.get("label", ""),
                font=dict(size=13), yanchor="top", align="right",
            )

    fig.update_layout(
        title=f"<b>{title}</b>", title_y=0.95, title_x=0.0,
        height=height, width=800,
        template="simple_white",
        font=dict(family="Arial, Helvetica, sans-serif", size=14),
        legend=dict(x=-0.05, y=1.3, xanchor="left", yanchor="top",
                    orientation="h", tracegroupgap=12),
        margin=dict(l=40, r=20, t=110, b=40),
        yaxis=dict(title="Share of windows", tickformat=".1%"),
        xaxis=dict(title="Date"),
    )
    return fig


# ── Figure 1: YouTube videos ──────────────────────────────────────────────────

print("Loading YouTube transcript data...")
ukip_raw   = pd.read_csv('../data/ukip_videos_with_meta.csv')
reform_raw = pd.read_csv('../data/reform_videos_with_meta.csv')
ukip_raw['party']   = 'UKIP'
reform_raw['party'] = 'Reform UK'

print("Computing linkage rates for YouTube videos (this may take a few minutes)...")
ukip_ts_cooc,   ukip_ts_any,   ukip_ts_causal   = calculate_equations(ukip_raw)
reform_ts_cooc, reform_ts_any, reform_ts_causal = calculate_equations(reform_raw)

events = [
    {"date": "2013-07-31", "label": "First GP Practice Closure"},
    {"date": "2016-06-23", "label": "Brexit Referendum"},
    {"date": "2021-01-31", "label": "Reform UK Founded"},
]

fig_youtube = plot_two_parties(
    ukip=(ukip_ts_cooc, ukip_ts_any, ukip_ts_causal),
    reform=(reform_ts_cooc, reform_ts_any, reform_ts_causal),
    title="",
    smooth_window=5,
    low_volume_threshold=400,
    events=events,
    palette={"UKIP": "#70147A", "Reform UK": "#00A2A1"},
)
fig_youtube.write_image(OUTPUT_DIR + 'fig7_immigration_NHS_linkage_YouTube.png')
print(f"Saved {OUTPUT_DIR}fig7_immigration_NHS_linkage_YouTube.png")


# ── Figure 2: UKIP press releases ────────────────────────────────────────────

print("Loading UKIP press release data...")
press_df = pd.read_parquet('../data/ukip_press_releases.parquet')
press_df['date'] = pd.to_datetime(press_df['date'])
press_df = press_df.reset_index().rename(columns={'index': 'doc_id'})
press_df['text'] = press_df['text'].astype(str)
press_df = press_df.loc[press_df['text'].notna()]

print("Annotating press releases (this may take a few minutes)...")
sents, docs = annotate_press_releases(press_df)
ukip_press_ts_cooc, ukip_press_ts_any, ukip_press_ts_causal = docs_to_monthly_timeseries(docs)

fig_press = plot_two_parties(
    ukip=(ukip_press_ts_cooc, ukip_press_ts_any, ukip_press_ts_causal),
    reform=None,
    title="",
    smooth_window=20,
    low_volume_threshold=40,
    events=[
        {"date": "2013-07-31", "label": "First GP Closure"},
        {"date": "2016-06-23", "label": "Brexit Referendum"},
    ],
)
fig_press.write_image(OUTPUT_DIR + 'figA11_immigration_NHS_linkage_press_releases.png')
print(f"Saved {OUTPUT_DIR}figA11_immigration_NHS_linkage_press_releases.png")
