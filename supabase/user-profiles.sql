create table if not exists public.user_profiles (
  id uuid primary key,
  email text not null unique,
  full_name text not null,
  phone text,
  metros text[] not null default '{}',
  role text not null default 'viewer',
  created_at timestamptz not null default now()
);

alter table public.user_profiles disable row level security;
