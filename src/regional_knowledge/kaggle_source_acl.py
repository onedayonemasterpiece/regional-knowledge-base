"""Fixed native safe-role/ACL observation; no controller authority."""
import json

class NativeHistoryError(RuntimeError):pass

def _json(value):
    return json.dumps(value,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(",",":")).encode()

ACL_SQL = """SELECT kind, name, payload FROM (
 SELECT 'role' kind, rolname name, jsonb_build_array(rolsuper,rolinherit,rolcreaterole,
    rolcreatedb,rolcanlogin,rolreplication,rolbypassrls,rolconnlimit,rolvaliduntil AT TIME ZONE 'UTC')::text payload
 FROM pg_roles
 UNION ALL SELECT 'membership',r.rolname||':'||m.rolname,
    jsonb_build_array(a.admin_option,a.inherit_option,a.set_option)::text
 FROM pg_auth_members a JOIN pg_roles r ON r.oid=a.roleid JOIN pg_roles m ON m.oid=a.member
 UNION ALL SELECT 'schema',nspname,jsonb_build_array(pg_get_userbyid(nspowner),nspacl)::text
 FROM pg_namespace WHERE nspname IN ('rkb_v4','public')
 UNION ALL SELECT 'table',n.nspname||'.'||c.relname,
    jsonb_build_array(pg_get_userbyid(c.relowner),c.relacl)::text
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='rkb_v4'
 UNION ALL SELECT 'column',c.relname||'.'||a.attname,a.attacl::text
 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='rkb_v4' AND a.attacl IS NOT NULL
 UNION ALL SELECT 'function',n.nspname||'.'||p.proname||':'||coalesce((
    SELECT string_agg(tn.nspname||'.'||t.typname,',' ORDER BY a.ordinal)
    FROM unnest(p.proargtypes) WITH ORDINALITY a(type_oid,ordinal)
    JOIN pg_type t ON t.oid=a.type_oid JOIN pg_namespace tn ON tn.oid=t.typnamespace),''),
    jsonb_build_array(pg_get_userbyid(p.proowner),p.proacl)::text
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='rkb_v4'
 UNION ALL SELECT 'default',pg_get_userbyid(d.defaclrole)||':'||
    CASE WHEN d.defaclnamespace=0 THEN '*' ELSE
        (SELECT nspname::text FROM pg_namespace WHERE oid=d.defaclnamespace) END||':'||
    d.defaclobjtype::text,d.defaclacl::text
 FROM pg_default_acl d
 UNION ALL SELECT 'database',datname,jsonb_build_array(pg_get_userbyid(datdba),datacl)::text
 FROM pg_database WHERE datname=current_database()
 UNION ALL SELECT 'setting',
    CASE WHEN setdatabase=0 THEN '*' ELSE (SELECT datname::text FROM pg_database WHERE oid=setdatabase) END||':'||
    CASE WHEN setrole=0 THEN '*' ELSE pg_get_userbyid(setrole) END,setconfig::text FROM pg_db_role_setting
 ) q ORDER BY kind COLLATE "C",name COLLATE "C" LIMIT 513"""


async def inspect_acl(connection):
    """Safe, bounded role/ACL bytes for deployment pinning; never pg_authid."""
    async with connection.cursor() as cursor:
        # Guard individual values on the server before transferring bytes.
        await cursor.execute('SELECT kind,name,CASE WHEN octet_length(payload)<=8192 THEN payload END '
            'FROM (' + ACL_SQL + ') bounded')
        rows = await cursor.fetchall()
    if len(rows) > 512 or any(not isinstance(r[2], str) for r in rows):
        raise NativeHistoryError('native role ACL metadata bound exceeded')
    data = _json(rows)
    if len(data) > 256 * 1024:
        raise NativeHistoryError('native role ACL byte bound exceeded')
    roles = {r[1]: json.loads(r[2]) for r in rows if r[0] == 'role'}
    if not {'rkb_bootstrap', 'rkb_writer', 'rkb_reader'} <= roles.keys():
        raise NativeHistoryError('native role closure missing')
    for name in ('rkb_writer', 'rkb_reader'):
        attrs = roles[name]
        if any(attrs[i] is not False for i in (0, 2, 3, 5, 6)):
            raise NativeHistoryError('native role privilege bypass')
    if any(r[0] == 'membership' and any(name in r[1].split(':')
           for name in ('rkb_writer', 'rkb_reader')) for r in rows):
        raise NativeHistoryError('native role membership bypass')
    return data

