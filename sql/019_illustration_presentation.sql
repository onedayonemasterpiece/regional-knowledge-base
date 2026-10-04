begin;

alter table public.rkb_illustrations
  add column if not exists display_rotation_degrees smallint not null default 0;
alter table public.rkb_illustrations
  add column if not exists display_crop_sha256 text;

update public.rkb_illustrations
set display_crop_sha256 = source_crop_sha256
where display_crop_sha256 is null
  and source_crop_sha256 is not null;

do $$
begin
  if not exists (
    select 1 from pg_constraint
    where conname = 'rkb_illustrations_display_rotation_cardinal'
  ) then
    alter table public.rkb_illustrations
      add constraint rkb_illustrations_display_rotation_cardinal
      check (display_rotation_degrees in (0,90,180,270));
  end if;
  if not exists (
    select 1 from pg_constraint
    where conname = 'rkb_illustrations_display_crop_sha256_format'
  ) then
    alter table public.rkb_illustrations
      add constraint rkb_illustrations_display_crop_sha256_format
      check (
        display_crop_sha256 is null
        or display_crop_sha256 ~ '^[a-f0-9]{64}$'
      );
  end if;
end
$$;

commit;
