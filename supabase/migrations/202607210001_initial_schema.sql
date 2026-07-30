-- Vigil attribution warehouse for Supabase Postgres.
--
-- Values intentionally remain TEXT where the existing SQLite warehouse uses
-- TEXT. This preserves current reporting semantics during the migration and
-- avoids silently rounding money, IDs, or timestamps.

create table if not exists public.spend (
  platform text,
  date text,
  account_id text,
  campaign_id text,
  adset_id text,
  ad_id text,
  creative_id text,
  clicks text,
  cost text,
  impressions text,
  metadata text,
  currency text
);

alter table public.spend add column if not exists currency text;

create table if not exists public.sessions (
  session_id text,
  visitor_id text,
  ts text,
  event_name text,
  page_title text,
  utm_source text,
  utm_medium text,
  utm_campaign text,
  utm_content text,
  utm_term text,
  referrer text,
  landing_page text,
  device text,
  gclid text,
  fbclid text,
  ttclid text,
  customer_key text,
  custom_data_json text
);

create table if not exists public.touchpoints (
  ts text,
  channel text,
  platform text,
  campaign_id text,
  adset_id text,
  ad_id text,
  creative_id text,
  gclid text,
  fbclid text,
  ttclid text,
  customer_key text,
  session_id text,
  visitor_id text default ''
);

create table if not exists public.orders (
  order_id text,
  ts text,
  gross text,
  net text,
  refunds text,
  chargebacks text,
  cogs text,
  fees text,
  customer_key text,
  subscription_id text,
  session_id text,
  visitor_id text,
  channel text,
  platform text,
  campaign_id text,
  adset_id text,
  ad_id text,
  creative_id text,
  gclid text,
  fbclid text,
  ttclid text,
  currency text,
  processor text,
  processor_customer_id text,
  payment_fingerprint text,
  product_key text,
  is_recurring integer default 0,
  sale_group_id text
);

create table if not exists public.conversions (
  conversion_id text,
  ts text,
  type text,
  value text,
  order_id text,
  customer_key text,
  session_id text,
  visitor_id text
);

create table if not exists public.reported_value (
  platform text,
  date text,
  account_id text,
  campaign_id text,
  adset_id text,
  ad_id text,
  conversion_type text,
  reported_value text
);

create table if not exists public.ad_names (
  platform text not null,
  entity_type text not null,
  entity_id text not null,
  name text,
  parent_id text,
  source text default 'manual',
  updated_at text,
  thumbnail_url text,
  creative_type text,
  video_id text,
  creative_id text,
  primary key (platform, entity_type, entity_id)
);

create table if not exists public.video_metrics (
  platform text,
  date text,
  account_id text,
  campaign_id text,
  adset_id text,
  ad_id text,
  creative_id text,
  video_id text,
  video_name text,
  views text default '0',
  views_3s text default '0',
  views_25 text default '0',
  views_50 text default '0',
  views_75 text default '0',
  views_100 text default '0',
  views_25pct text default '0',
  views_50pct text default '0',
  views_75pct text default '0',
  views_100pct text default '0',
  avg_watch_time_sec text default '0',
  video_length_sec text default '0',
  thumb_stop_rate text default '0',
  hook_rate text default '0',
  hold_rate text default '0',
  ctr text default '0',
  click_through_rate text default '0',
  cost text default '0',
  cost_per_view text default '0',
  impressions text default '0',
  clicks text default '0'
);

alter table public.video_metrics add column if not exists account_id text;
alter table public.video_metrics add column if not exists views_25 text default '0';
alter table public.video_metrics add column if not exists views_50 text default '0';
alter table public.video_metrics add column if not exists views_75 text default '0';
alter table public.video_metrics add column if not exists views_100 text default '0';
alter table public.video_metrics add column if not exists views_25pct text default '0';
alter table public.video_metrics add column if not exists views_50pct text default '0';
alter table public.video_metrics add column if not exists views_75pct text default '0';
alter table public.video_metrics add column if not exists views_100pct text default '0';
alter table public.video_metrics add column if not exists video_length_sec text default '0';
alter table public.video_metrics add column if not exists click_through_rate text default '0';
alter table public.video_metrics add column if not exists impressions text default '0';
alter table public.video_metrics add column if not exists clicks text default '0';

create table if not exists public.campaign_settings (
  platform text not null,
  campaign_id text not null,
  tracked integer not null default 1,
  note text default '',
  updated_at text,
  primary key (platform, campaign_id)
);

create table if not exists public.refund_log (
  id text primary key,
  ts text,
  order_id text,
  customer_key text,
  type text,
  amount text,
  reason text,
  source text
);

create table if not exists public.platform_tokens (
  platform text primary key,
  access_token text,
  refresh_token text,
  advertiser_id text,
  expires_at text,
  updated_at text
);

create table if not exists public.stripe_sync_coverage (
  start_date text not null,
  end_date text not null,
  updated_at text not null,
  primary key (start_date, end_date)
);

create table if not exists public.capi_log (
  id text primary key,
  ts text,
  platform text,
  event_name text,
  customer_key text,
  order_id text,
  value text,
  status text,
  response text,
  error text
);

create table if not exists public.webhook_log (
  id text primary key,
  ts text,
  source text,
  event_type text,
  payload text,
  result text
);

create table if not exists public.email_sms_events (
  id text primary key,
  ts text,
  customer_key text,
  channel text,
  event_type text,
  sequence_name text,
  step_number text,
  subject_line text,
  link_url text,
  source text
);

create index if not exists idx_sessions_session_id on public.sessions (session_id);
create index if not exists idx_sessions_visitor_id on public.sessions (visitor_id);
create index if not exists idx_sessions_customer_key on public.sessions (customer_key);
create index if not exists idx_sessions_ts on public.sessions (ts);

create index if not exists idx_touchpoints_customer_ts on public.touchpoints (customer_key, ts);
create index if not exists idx_touchpoints_session_id on public.touchpoints (session_id);
create index if not exists idx_touchpoints_visitor_id on public.touchpoints (visitor_id);
create index if not exists idx_touchpoints_customer_key on public.touchpoints (customer_key);
create index if not exists idx_touchpoints_ts on public.touchpoints (ts);
create index if not exists idx_touchpoints_campaign_id on public.touchpoints (campaign_id);

create unique index if not exists idx_orders_order_id_unique on public.orders (order_id);
create index if not exists idx_orders_customer_key on public.orders (customer_key);
create index if not exists idx_orders_session_id on public.orders (session_id);
create index if not exists idx_orders_visitor_id on public.orders (visitor_id);
create index if not exists idx_orders_ts on public.orders (ts);
create index if not exists idx_orders_sale_group_id on public.orders (sale_group_id);
create index if not exists idx_orders_processor_customer_id on public.orders (processor_customer_id);
create index if not exists idx_orders_payment_fingerprint on public.orders (payment_fingerprint);
create index if not exists idx_orders_is_recurring on public.orders (is_recurring);

create unique index if not exists idx_conversions_conversion_id_unique on public.conversions (conversion_id);
create index if not exists idx_conversions_customer_key on public.conversions (customer_key);
create index if not exists idx_conversions_ts on public.conversions (ts);
create index if not exists idx_conversions_order_id on public.conversions (order_id);

create index if not exists idx_spend_date on public.spend (date);
create index if not exists idx_spend_platform_date on public.spend (platform, date);
create index if not exists idx_video_metrics_date on public.video_metrics (date);
create index if not exists idx_video_metrics_platform_date on public.video_metrics (platform, date);
create index if not exists idx_reported_value_date on public.reported_value (date);
create index if not exists idx_reported_value_platform_date on public.reported_value (platform, date);
create index if not exists idx_refund_log_order_ts on public.refund_log (order_id, ts);
create index if not exists idx_refund_log_ts on public.refund_log (ts);
create index if not exists idx_capi_log_order_status on public.capi_log (order_id, status);
create index if not exists idx_capi_log_platform_status on public.capi_log (platform, status);
create index if not exists idx_webhook_log_source_ts on public.webhook_log (source, ts);
create index if not exists idx_email_sms_events_customer_ts on public.email_sms_events (customer_key, ts);

insert into public.campaign_settings (platform, campaign_id, tracked, note, updated_at)
values
  ('google', '21892266666', 0, 'Default Hyros parity exclusion', null),
  ('google', '23351447961', 0, 'Default Hyros parity exclusion', null),
  ('meta', '120237922106660149', 0, 'Default Hyros parity exclusion', null)
on conflict (platform, campaign_id) do nothing;

-- The browser publishable key must not have direct warehouse access. The
-- backend connects with a private Postgres role and remains the API boundary.
alter table public.spend enable row level security;
alter table public.sessions enable row level security;
alter table public.touchpoints enable row level security;
alter table public.orders enable row level security;
alter table public.conversions enable row level security;
alter table public.reported_value enable row level security;
alter table public.ad_names enable row level security;
alter table public.video_metrics enable row level security;
alter table public.campaign_settings enable row level security;
alter table public.refund_log enable row level security;
alter table public.platform_tokens enable row level security;
alter table public.stripe_sync_coverage enable row level security;
alter table public.capi_log enable row level security;
alter table public.webhook_log enable row level security;
alter table public.email_sms_events enable row level security;
