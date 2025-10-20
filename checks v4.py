import datetime
import pandas as pd
import requests
import psycopg2
from psycopg2 import OperationalError
import warnings
import os
warnings.filterwarnings("ignore")

#year_month = datetime.datetime.now().strftime('%Y%m') # Si el anticipo se calcula el mes anterior
year_month = (datetime.datetime.now().replace(day=1) - datetime.timedelta(days=1)).strftime('%Y%m') # Si el anticipo se calcula el mes actual

payout_date = (datetime.datetime.now().replace(day=1)).replace(day=25).strftime('%Y-%m-%d') # Si el anticipo se calcula el mes actual
#payout_date = (datetime.datetime.now().replace(day=1) + datetime.timedelta(days=32)).replace(day=25).strftime('%Y-%m-%d') # Si el anticipo se calcula el mes anterior

def create_connection(db_name, db_user, db_password, db_host, db_port):
    conn = None
    try:
        conn = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
            options="-c client_encoding=UTF8"
        )
        print("Conexión a PostgreSQL exitosa")
    except OperationalError as e:
        print(f"El error '{e}' ocurrió")
    return conn

#Conexión BD
db_name, db_user, db_password, db_host, db_port = (
    "MattiProductionDb", "juan.garcia", "Xr&Rd<4nuSG/+z2e",
    "matti-production-aurora-cluster.cluster-cu0j0dopeo4q.us-east-1.rds.amazonaws.com", "5432" )

output_excel = "C:/Users/carmen.galvez/Downloads/Checks_Anticipos.xlsx"

campus_ids = ['7971152b-e8fa-4d5e-af17-f63c3c56a2a7',
                    '70c49bf2-d619-43a1-b6bd-38d72f226608',
                    '3027fa98-b12b-45a5-a77b-536979476709',
                    '5e272ef7-ac05-467e-8aa2-20654bce2ed9',
                    '4d7b8857-a909-4248-941a-5e5b9e0880e6',
                    'b56ca936-1a9d-4651-8833-a2c69147f3a6',
                    '25a7e77f-63df-4209-81b7-b42a6f7dae70',
                    'ac5cc5d2-c023-47c0-af96-dcc05c9087a6',
                    'edfab51c-bb1d-4521-b143-c8fe1be96610',
                    '5c347638-b762-40b4-ac00-0ddeeb94a2f1',
                    '2c016e77-dbc6-4d50-b14e-f5714a80e873',
                    '9027ad39-8bf7-4e82-b3fd-0877d62f1e61',
                    '31693b9d-2754-49f2-90fc-6ace3b777f84',
                    '13142ded-fdbb-4a88-83c8-c65e53cd7fef',
                    '907c658e-62d6-4168-9ae8-a223969ba845',
                    'a974bdb9-bac8-41f1-865e-391411dae6d6',
                    '984cd37e-d3fb-41dd-bde6-462da3b862a4',
                    '9c8d363a-3c74-47d2-885b-8a2fb37d09c4',
                    'af80843a-a4bb-4453-a642-b22b55cde453',
                    'fc3c7c66-e981-4adc-a54c-6c06aad799d0',
                    'cd07df3e-e3cc-4a60-a02a-c746fca895ac',
                    'f5afa588-23a6-4409-ad87-4080b4e02a3d',
                    'd2fbdaeb-847d-450a-9169-e6961040ebcf',
                    'dbfedb7f-abdd-4a91-ab08-d47f9f19a0f5',
                    'e4e1ae64-bb3f-4c78-a3d9-b6eec81d84d1',
                    'ec2fa058-3c04-4910-9346-5d67cf1aa998'
]

str_campuses =  ', '.join([f"'{campus_id}'" for campus_id in campus_ids])
errores_por_campus = {campus_id: 0 for campus_id in campus_ids}

connection = create_connection(db_name, db_user, db_password, db_host, db_port)

# Resumen checks, muestra incidencias por campus
summary_rows = []

def build_payout_to_campus(conn, payout_ids_sql):
    """
    Retorna dict {payout_id -> campus_id} para los payouts del mes/fecha actual.
    payout_ids_sql debe ser un subquery tipo: 'select id from payouts_factoring ...'
    """
    try:
        df_map = pd.read_sql(f"select id as payout_id, campus_id from payouts_factoring where id in ({payout_ids_sql})", conn)
        return dict(zip(df_map['payout_id'].astype(str), df_map['campus_id']))
    except Exception:
        return {}

def invoice_to_campus(conn, invoice_ids):
    """
    Retorna dict {invoice_id -> campus_id} para una lista de invoice_ids.
    """
    if not invoice_ids:
        return {}
    ids = ', '.join([f"'{x}'" for x in invoice_ids])
    df_map = pd.read_sql(f"select id as invoice_id, campus_id from invoices where id in ({ids})", conn)
    return dict(zip(df_map['invoice_id'].astype(str), df_map['campus_id']))

def ensure_campus_col(df, payout_to_campus=None, conn=None): #innecesario
    """
    Asegura la columna campus_id en df usando payout_id o invoice_id si hace falta.
    No modifica queries; sólo enriquece el dataframe.
    """
    if df is None or df.empty:
        return df
    if 'campus_id' in df.columns:
        return df
    df = df.copy()
    if 'payout_id' in df.columns and payout_to_campus:
        df['campus_id'] = df['payout_id'].astype(str).map(payout_to_campus)
    # Si todavía no hay campus y hay invoice_id, consultamos invoices para mapear
    if 'campus_id' not in df.columns and 'invoice_id' in df.columns and conn is not None:
        invs = [str(x) for x in df['invoice_id'].dropna().astype(str).unique().tolist()]
        inv_map = invoice_to_campus(conn, invs)
        df['campus_id'] = df['invoice_id'].astype(str).map(inv_map)
    return df

def add_summary(check_name, df, payout_to_campus=None, conn=None):
    """
    Agrega filas a summary_rows con el conteo de coincidencias por campus_id para el 'check_name'.
    Si df está vacío o no se pudo inferir campus_id, se agrega 0 para todos los campus.
    """
    if df is None or df.empty:
        for c in campus_ids:
            summary_rows.append({'check': check_name, 'campus_id': c, 'coincidencias': 0})
        return

    df2 = ensure_campus_col(df, payout_to_campus=payout_to_campus, conn=conn)

    if 'campus_id' not in df2.columns:
        for c in campus_ids:
            summary_rows.append({'check': check_name, 'campus_id': c, 'coincidencias': 0})
        return

    vc = df2['campus_id'].value_counts()
    for c in campus_ids:
        cnt = int(vc.get(c, 0))
        summary_rows.append({'check': check_name, 'campus_id': c, 'coincidencias': cnt})


with pd.ExcelWriter(output_excel, engine='xlsxwriter') as excel_writer:

    payout_ids = f"select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}'"
    payout_saas_ids = f"select payout_saas_id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}'"
    alerta=" 🚨Continuar proceso manualmente \n "

    #1. Query to check if the payout are completed
    query = f"""select * from payouts_factoring where campus_id in ({str_campuses}) and status <> 'completed' and f_calculate_date_yearmonth(payout_date::date) <= {year_month}"""
    df_factoring = pd.read_sql_query(query, connection)
    # Crear un diccionario para mapear campus_id a campus_name
    query_campus_names = """
    SELECT id AS campus_id, name AS campus_name
    FROM campuses
    WHERE id IN ({str_campuses})
    """
    df_campus_names = pd.read_sql_query(query_campus_names.format(str_campuses=str_campuses), connection)
    campus_name_map = dict(zip(df_campus_names['campus_id'], df_campus_names['campus_name']))


    if len(df_factoring) > 0:
        for campus_id in df_factoring['campus_id']:
            errores_por_campus[campus_id] += 1
        df_factoring.to_excel(excel_writer, sheet_name='Payouts_No_Completados', index=False)
        print('Payout not completed yet del campus_id(s):', df_factoring['campus_id'].unique().tolist())

    # Actualizar str_campuses excluyendo los campus con payouts no completados
    if len(df_factoring) > 0:
        campus_no_completados = df_factoring['campus_id'].unique().tolist()
        campus_filtrados = [campus for campus in campus_ids if campus not in campus_no_completados]
        str_campuses = ', '.join([f"'{campus_id}'" for campus_id in campus_filtrados])
        print(f"Campus excluidos por payouts no completados: {campus_no_completados}")
        print(f"Campus restantes para continuar checks: {len(campus_filtrados)}")
    else:
        # Si no hay payouts no completados, usar todos los campus originales
        str_campuses = ', '.join([f"'{campus_id}'" for campus_id in campus_ids])

    #Obtener el periodo del anticipo
    query_campus = f"""select c.id as campus_id, c.complements_factoring, c.inscriptions_factoring, p.start_date, p.end_date, p.id as period_id
    from campuses c join campus_periods p
    on c.id=p.campus_id
    where campus_id in ({str_campuses})
    AND CURRENT_DATE BETWEEN p.start_date AND p.end_date """

    df_campus = pd.read_sql_query(query_campus, connection)
    periodo = f"select id from campus_periods where campus_id in ({str_campuses}) AND CURRENT_DATE BETWEEN start_date AND end_date"
    period_ids = df_campus['period_id'].unique().tolist()
    str_periods = ', '.join([f"'{period_id}'" for period_id in period_ids])

    #2. Resumen del campus, muestra informaión relevante del campus 
    query_resumen = f"""select c.id,
       c.name,
       c.country,
       case
           when
               (c.complements_factoring = true and c.inscriptions_factoring = true)
               then 'membership, complment & inscription'
           when (c.complements_factoring = true) then 'membership & complement'
           when (c.inscriptions_factoring = true) then 'membership & inscription'
           else 'membership'
           end                            as conceptos_anticipable,
       c.with_resources,
       array_agg(distinct i.concept_type) as conceptos_en_base
from campuses c
         left join invoices i on c.id = i.campus_id
where f_calculate_date_yearmonth(date_period) = {year_month}
    and c.id in ({str_campuses})
  and i.status in ('successful', 'expired', 'pending')
group by c.id,
         c.name,
         c.country,
         case
             when
                 (c.complements_factoring = true and c.inscriptions_factoring = true)
                 then 'membership, complement & inscription'
             when (c.complements_factoring = true) then 'membership & complement'
             when (c.inscriptions_factoring = true) then 'membership & inscription'
             else 'membership'
             end,
         c.with_resources;"""
    df_resumen = pd.read_sql_query(query_resumen, connection)
    df_resumen.to_excel(excel_writer, sheet_name='Resumen', index=False)

    #3. Obtener conceptos anticipables, MEJORAR
    query_invoices= f"""
    select p.campus_id, f.payout_id, f.student_id, f.invoice_id, f.concept_type, f.factoring, f.details from payouts_factoring_invoices f
    join payouts_factoring p on p.id = f.payout_id
    where payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}')
    """

    df_campus=pd.read_sql_query(query_campus, connection)
    df_invoices=pd.read_sql_query(query_invoices, connection)
    df_complements=df_invoices[df_invoices['concept_type'] != 'membership']
    df_campus_complements = pd.DataFrame({'campus_id': df_complements['campus_id'].unique()})
    str_alumnos_anticipados = ', '.join([f"'{student_id}'" for student_id in df_invoices[df_invoices['details'].isin(['activo','deudor','factoring false'])]['student_id'].unique()])

    #Ver que las banderas esten correctas
    query_banderas_incorrectas = f"""
        select i.id, i.concept_type, i.campus_id, i.factoring from invoices i
    left join campus_students cs on cs.student_id = i.student_id and cs.campus_id = i.campus_id
    left join campuses c on cs.campus_id = c.id
    left join payouts_factoring_invoices pfi on pfi.invoice_id = i.id
    where i.campus_id in ({str_campuses})
    and i.factoring = false
    and f_calculate_date_yearmonth(i.date_period) = {year_month} + 1
    and i.status not in ('deleted')
    and pfi.details not in ('inactivo', 'deudor')
    and case
        when
        (c.complements_factoring = true and c.inscriptions_factoring = true) then i.concept_type in ('membership', 'complement', 'inscription')
        when (c.complements_factoring = true) then i.concept_type in ('membership', 'complement')
        when (c.inscriptions_factoring = true) then i.concept_type in ('membership', 'inscription')
        else i.concept_type in ('membership')
        end
    and i.student_id not in (SELECT student_id
            FROM invoices
            WHERE student_id IN (
                SELECT student_id
                FROM invoices
                WHERE status IN ('pending','expired','successful')
                AND campus_id in ({str_campuses})
                AND concept_type = 'membership'
                AND campus_period_id = (
                    SELECT id
                    FROM campus_periods
                    WHERE campus_id = i.campus_id
                    ORDER BY start_date DESC
                    LIMIT 1
                )
                GROUP BY student_id
                HAVING count(*) = sum(CASE WHEN status = 'successful' THEN 1 ELSE 0 END)
            )
            AND f_calculate_date_yearmonth(date_period) = f_calculate_date_yearmonth('{payout_date}'::date)
            AND campus_id in ({str_campuses}));"""
    df_banderas_incorrectas = pd.read_sql_query(query_banderas_incorrectas, connection)
    if len(df_banderas_incorrectas) > 0:
        invoice_ids = df_banderas_incorrectas['id'].tolist()
        str_invoices = ', '.join([f"'{inv}'" for inv in invoice_ids])
        update_sql = f"UPDATE invoices SET factoring = true WHERE id IN ({str_invoices});"
        # Solo la primera fila tendrá el update, las demás estarán vacías
        accionable_col = [update_sql] + [''] * (len(df_banderas_incorrectas) - 1)
        df_banderas_incorrectas_ex = pd.DataFrame({
            'campus_id': df_banderas_incorrectas['campus_id'],
            'campus_name': df_banderas_incorrectas['campus_id'].map(campus_name_map),
            'invoice_id': df_banderas_incorrectas['id'],
            'concept_type': df_banderas_incorrectas['concept_type'],
            'factoring': df_banderas_incorrectas['factoring'],
            'accionable': accionable_col
        })
        df_banderas_incorrectas_ex.to_excel(excel_writer, sheet_name='Banderas_Incorrectas', index=False)
        for campus_id in df_banderas_incorrectas['campus_id']:
            errores_por_campus[campus_id] += 1
    add_summary('Banderas_Incorrectas', df_banderas_incorrectas, conn=connection)

    #ANTICIPO
    #3. Query deuda(de otros periodos, de otros campus y por concepto)
    query_deudores=f"""WITH vencidas AS (
    SELECT student_id, campus_id,
           COUNT(id) AS vencidas_count
    FROM invoices
    WHERE status = 'expired'
      AND concept_type = 'membership'
      AND campus_id NOT IN (
        '850c1e66-4b7e-48f1-88e0-f1c51c5245ca',
        'ffb8f970-a4fc-4ada-8409-9d734bf9e9ff',
        '5cc2a86c-9719-4ea7-8c0f-8bbbcddc011a',
        '39c8e655-55bf-41c1-9c66-a890edd50e47'
      )
    GROUP BY student_id, campus_id
    ),
    deudores AS (
        SELECT student_id,
            COUNT(DISTINCT campus_id) AS colegios_vencidos
        FROM vencidas
        WHERE vencidas_count > 0
        GROUP BY student_id
    ),
    vencidas_3m AS (
        SELECT student_id, campus_id, COUNT(*) AS vencidas_3m_count
        FROM invoices
        WHERE status = 'expired'
        AND concept_type = 'membership'
        GROUP BY student_id, campus_id
        HAVING COUNT(*) > 2
    ),
    deudores_ciclo_pasado AS (
        SELECT student_id, campus_id, COUNT(id) AS vencidas_ciclo_pasado
        FROM invoices
        WHERE status = 'expired'
        AND concept_type = 'membership'
        AND campus_period_id NOT IN ({str_periods})
        GROUP BY student_id, campus_id
    )
    SELECT cs.student_id,
        cs.campus_id,
        COALESCE(v.vencidas_count, 0) AS vencidas_count,
        cs.is_defaulter               AS original_is_defaulter,
        -- Deudor por más de 1 campus
        CASE
            WHEN ds.colegios_vencidos > 1 THEN TRUE
            ELSE FALSE
        END AS defaulter_by_campus,
        -- Deudor por más de 3 meses
        CASE
            WHEN v3m.student_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS defaulter_por_mas_de_3_meses,
        -- Deudor por ciclo pasado
        CASE
            WHEN dcp.student_id IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS defaulter_ciclo_pasado,
        -- OR de todas las condiciones anteriores
        CASE
            WHEN (ds.colegios_vencidos > 1
                    OR v3m.student_id IS NOT NULL
                    OR dcp.student_id IS NOT NULL)
            THEN TRUE
            ELSE FALSE
        END AS calculated_is_defaulter
    FROM campus_students cs
    LEFT JOIN deudores ds
        ON cs.student_id = ds.student_id
    LEFT JOIN vencidas v
        ON cs.student_id = v.student_id
        AND cs.campus_id = v.campus_id
    LEFT JOIN vencidas_3m v3m
        ON cs.student_id = v3m.student_id
        AND cs.campus_id = v3m.campus_id
    LEFT JOIN deudores_ciclo_pasado dcp
        ON cs.student_id = dcp.student_id
        AND cs.campus_id = dcp.campus_id
    WHERE cs.status = 'active'
    AND cs.campus_id NOT IN (
            '850c1e66-4b7e-48f1-88e0-f1c51c5245ca',
            'ffb8f970-a4fc-4ada-8409-9d734bf9e9ff',
            '5cc2a86c-9719-4ea7-8c0f-8bbbcddc011a',
            '39c8e655-55bf-41c1-9c66-a890edd50e47'
    )
    AND cs.campus_id IN ({str_campuses})
    AND cs.student_id IN ({str_alumnos_anticipados})
    ORDER BY cs.student_id, cs.campus_id;"""

    df_is_defaulter=pd.read_sql_query(query_deudores, connection)

    # Obtener deudores según el query
    df_deudores = df_is_defaulter[df_is_defaulter['calculated_is_defaulter'] == True]

    # Obtener deudores por campus en el anticipo
    deudores_por_campus = df_deudores.groupby('campus_id')['student_id'].apply(set).to_dict()
    deudores_reales_por_campus = df_deudores.groupby('campus_id')['student_id'].apply(set).to_dict()

    df_invoices_deudores = df_invoices[df_invoices['details'] == 'deudor']

    deudores_anticipo_por_campus = df_invoices_deudores.groupby('campus_id')['student_id'].apply(set).to_dict()

    # Comparar por campus
    for campus_id in campus_ids:
        deudores_reales = deudores_reales_por_campus.get(campus_id, set())
        deudores_anticipo = deudores_anticipo_por_campus.get(campus_id, set())
        
        if deudores_reales != deudores_anticipo:
            diferentes_query = deudores_reales.difference(deudores_anticipo)
            diferentes_anticipo = deudores_anticipo.difference(deudores_reales)
            errores_por_campus[campus_id] += 1
        
            # Guardar detalles en Excel
            df_deudores_ex = pd.DataFrame({
                'campus_id': [campus_id],
                'student_ids_diferentes_query': [list(diferentes_query)],
                'student_ids_diferentes_anticipo': [list(diferentes_anticipo)]

            })
            df_deudores_ex.to_excel(excel_writer, sheet_name='Deudores_No_Coinciden', index=False, header=True)
    print("Checks deudores realizados")

    rows_deudores_diff = []
    for c in campus_ids:
        deudores_reales = deudores_reales_por_campus.get(c, set())
        deudores_anticipo = deudores_anticipo_por_campus.get(c, set())
        if deudores_reales != deudores_anticipo:
            diferentes_query = list(deudores_reales.difference(deudores_anticipo))
            diferentes_anticipo = list(deudores_anticipo.difference(deudores_reales))
            rows_deudores_diff.append({
                'campus_id': c,
                'campus_name': campus_name_map.get(c, ''),
                'student_ids_diferentes_query': diferentes_query,
                'student_ids_diferentes_anticipo': diferentes_anticipo
            })

    if rows_deudores_diff:
        df_deudores_ex = pd.DataFrame(rows_deudores_diff)
        df_deudores_ex.to_excel(excel_writer, sheet_name='Deudores_No_Coinciden', index=False)
        add_summary('Deudores_No_Coinciden', df_deudores_ex, conn=connection)


    #4. Anticipo check de monto--AQUÍ FALTA check de amount payout_factoring este es de ajustes
    query_monto_anticipable = f"""
    select 
        f.campus_id, 
        c.name as campus_name, 
        d.payout_id, 
        (adjustments_main_concept + students_registered_amount + students_deregistered_amount) as total_adjustments, 
        sum(amount_adjusted) as sum_adjusted
    from payouts_factoring_details d
    join payouts_factoring_adjustments p on d.payout_id = p.payout_id
    join payouts_factoring f on f.id = p.payout_id
    join campuses c on f.campus_id = c.id
    where p.payout_id in (
        select id 
        from payouts_factoring 
        where campus_id in ({str_campuses}) 
        and payout_date = '{payout_date}'
    )
    group by 
        d.payout_id, 
        f.campus_id, 
        c.name, 
        (adjustments_main_concept + students_registered_amount + students_deregistered_amount)
    having 
        ((adjustments_main_concept + students_registered_amount + students_deregistered_amount) - sum(amount_adjusted) > 1)
        or 
        ((adjustments_main_concept + students_registered_amount + students_deregistered_amount) - sum(amount_adjusted) < -1);
    """

    df_monto_anticipable = pd.read_sql_query(query_monto_anticipable, connection)
    if len(df_monto_anticipable) > 0:
        df_monto_anticipable.to_excel(excel_writer, sheet_name='Monto_Anticipable', index=False)
        for campus_id in df_monto_anticipable['campus_id']:
            errores_por_campus[campus_id] += 1
        add_summary('Monto_Anticipable', df_monto_anticipable, conn=connection)

    print("Checks Resumen realizados")
 
    #5. Anualidades, HOMOLOGAR CON NO_ANTICPABLES
    rows = []
    anualidad = []

    for campus_id in campus_ids:
        query_anualidad = f"""
        SELECT *
        FROM invoices
        WHERE student_id IN (
            SELECT student_id
            FROM invoices
            WHERE status IN ('pending','expired','successful')
            AND campus_id = '{campus_id}'
            AND concept_type = 'membership'
            AND campus_period_id = (
                SELECT id
                FROM campus_periods
                WHERE campus_id = '{campus_id}'
                ORDER BY start_date DESC
                LIMIT 1
            )
            GROUP BY student_id
            HAVING count(*) = sum(CASE WHEN status = 'successful' THEN 1 ELSE 0 END)
        )
        AND f_calculate_date_yearmonth(date_period) = f_calculate_date_yearmonth('{payout_date}'::date)
        AND campus_id = '{campus_id}';
        """

        df_anualidad = pd.read_sql_query(query_anualidad, connection)
        lista_anualidades= df_anualidad['id'].astype(str).tolist()

        if len(df_anualidad) > 0:
            anualidad.append({
                'campus_id': campus_id,
                'campus_name': campus_name_map.get(campus_id, ''),
                'id': lista_anualidades
            })

        lista_students_anualidades = df_anualidad['student_id'].unique().tolist()
        str_anualidades = ', '.join([f"'{s}'" for s in lista_students_anualidades]) if lista_students_anualidades else None

        # Consultar conceptos NO anticipables en payouts_factoring_invoices para ESTE campus
        payout_subquery = f"(SELECT id FROM payouts_factoring WHERE campus_id = '{campus_id}' AND payout_date = '{payout_date}')"

        if str_anualidades:
            query_no_anticipable = f"""
            SELECT payout_id, student_id, invoice_id, concept_type, factoring, details
            FROM payouts_factoring_invoices
            WHERE payout_id IN {payout_subquery}
            AND factoring = FALSE
            AND concept_type = 'membership'
            AND student_id IN ({str_anualidades})
            AND (details IS NULL OR details NOT IN ('deudor'));
            """
        else:
            # Si no hay anualidades, entonces no aplicamos el NOT IN (...) y devolvemos todos los no-factorable (excepto deudores)
            query_no_anticipable = f"""
            SELECT payout_id, student_id, invoice_id, concept_type, factoring, details
            FROM payouts_factoring_invoices
            WHERE payout_id IN {payout_subquery}
            AND factoring = FALSE
            AND concept_type = 'membership'
            AND (details IS NULL OR details NOT IN ('deudor'));
            """

        df_no_anticipable = pd.read_sql_query(query_no_anticipable, connection)

        # lista de invoice ids que se consideran "no anticipables" y se deberían marcar factoring = true
        lista_no_anticipables = df_no_anticipable['invoice_id'].astype(str).tolist()

        if len(lista_no_anticipables) > 0:
            str_no_anticipables = ', '.join([f"'{inv}'" for inv in lista_no_anticipables])
            update_no_anticipables = f"UPDATE invoices SET factoring = TRUE WHERE id IN ({str_no_anticipables})"
            errores_por_campus[campus_id] += 1

            # Guardar fila para Excel
            rows.append({
                'campus_id': campus_id,
                'campus_name': campus_name_map.get(campus_id, ''),
                'invoice_ids': lista_no_anticipables,
                'count': len(lista_no_anticipables),
                'update_sql': update_no_anticipables
            })
        if rows:
            df_rows = pd.DataFrame(rows)
            # Esto creará (o sobrescribirá) la hoja 'No_Anticipables' con la tabla de resultados
            df_rows.to_excel(excel_writer, sheet_name='No_Anticipables', index=False)
            # Si sigues usando excel_writer._save() en tu flujo, lo dejo para compatibilidad
            add_summary('No_Anticipables', df_rows, conn=connection)

        if anualidad:
            df_annuity = pd.DataFrame(anualidad)
            df_annuity.to_excel(excel_writer, sheet_name='Anualidades', index=False)
            add_summary('Anualidades', df_annuity, conn=connection)

    print("Checks anualidades realizados")

            
        #6. Casos estatus sin definir

    query_sin_definir= f"""select * from payouts_factoring_invoices 
        join payouts_factoring p on p.id = payout_id
        where (details='student not found'
        or ( details='inactivo' and factoring=true))
        and payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}');
        """

    df_sin_definir = pd.read_sql_query(query_sin_definir, connection) 

    df_preloaded = df_sin_definir[df_sin_definir['details'] == 'student_not_found']
    df_inactivos = df_sin_definir[df_sin_definir['details'] != 'student_not_found']
    rows = []

    if len(df_preloaded) > 0:
                for student_id in df_preloaded['student_id']:
                    rows.append({
                        'campus_id': campus_id,
                        'campus_name': campus_name_map.get(campus_id, ''),
                        'tipo': 'Preloaded',
                        'student_id': student_id,
                        'update_sql': 'N/A'
                    })
                errores_por_campus[campus_id] += 1
                add_summary('Preloaded', df_preloaded, conn=connection)


    if len(df_inactivos) > 0:
            lista_inactivos = df_inactivos['invoice_id'].to_list()
            str_inactivos = ', '.join([f"'{invoices}'" for invoices in lista_inactivos])
            update_inactivos = f"UPDATE invoices SET factoring = false WHERE id IN ({str_inactivos})"
            errores_por_campus[campus_id] += 1
            add_summary('Inactivos', df_inactivos, conn=connection)

            for campus_id, student_id in zip(df_inactivos['campus_id'], df_inactivos['student_id']):
                rows.append({
                    'campus_id': campus_id,
                    'campus_name': campus_name_map.get(campus_id, ''),
                    'tipo': 'Solicita update inactivos',
                    'student_id': student_id,
                'update_sql': update_inactivos
            })

    if rows:
            df_updates = pd.DataFrame(rows)
            df_updates.to_excel(excel_writer, sheet_name='Updates_Sin_Definir', index=False)


        #Check invoices con montos cero y alumno activo
    query_activo_cero = f"""
        select * from campus_students
        where campus_id in ({str_campuses})
        and status = 'active'
        and student_id in (
            select student_id from campus_students_periods
            where period_id in (
                select id from campus_periods
                where campus_id in ({str_campuses})
                AND CURRENT_DATE BETWEEN start_date AND end_date
            )
        )
        and student_id not in (
            select student_id from payouts_factoring_invoices
            where payout_id in (
                select id from payouts_factoring
                where campus_id in ({str_campuses}) and payout_date='{payout_date}'
            )
        )
        and student_id in (
            select student_id from invoices
            where status not in ('deleted')
            and campus_period_id in (
                select id from campus_periods
                where campus_id in ({str_campuses})
                AND CURRENT_DATE BETWEEN start_date AND end_date
            )
        );
        """

    df_activo_cero = pd.read_sql_query(query_activo_cero, connection)

    query_activo_cero_invoice = f"""
        select * from payouts_factoring_invoices 
        join payouts_factoring p on p.id = payout_id
        where initial_amount=0
        and details='activo'
        and payout_id in ({payout_ids});
        """

    df_activo_cero_invoice = pd.read_sql_query(query_activo_cero_invoice, connection)

    if len(df_activo_cero) > 0 or len(df_activo_cero_invoice) > 0:
        df_activo_cero['tipo'] = 'Alumno activo sin invoice'
        df_activo_cero_invoice['tipo'] = 'Invoice creada en cero'
        columnas = ['campus_id', 'student_id', 'invoice_id', 'tipo']
        for col in columnas:
            if col not in df_activo_cero_invoice.columns:
                df_activo_cero_invoice[col] = None
            if col not in df_activo_cero.columns:
                df_activo_cero[col] = None
        df_activo_cero = df_activo_cero[columnas]
        df_activo_cero_invoice = df_activo_cero_invoice[columnas]
        df_activo_cero = df_activo_cero.reset_index(drop=True)
        df_activo_cero_invoice = df_activo_cero_invoice.reset_index(drop=True)
        df_activo_cero_export = pd.concat([df_activo_cero, df_activo_cero_invoice], ignore_index=True)
        df_activo_cero_export = df_activo_cero_export.sort_values('campus_id')
        df_activo_cero_export['campus_name'] = df_activo_cero_export['campus_id'].map(campus_name_map)

        # === CONTADOR DE ERRORES POR CAMPUS ===
        for campus in df_activo_cero_export['campus_id'].unique():
            errores_por_campus[campus] += 1

        df_activo_cero_export.to_excel(excel_writer, sheet_name='Activos_Cero', index=False)
        add_summary('Activos_Cero', df_activo_cero_export, conn=connection)


        #Check banderas de invoices
        query_banderas = f"""select pfi.* from payouts_factoring_invoices pfi left join payouts_factoring_invoices_snapshot pfis on pfi.invoice_id = pfis.invoice_id
        where pfi.details in ('deudor', 'inactivo') and pfi.factoring = true
        and pfi.payout_id in ({payout_ids})
        and pfis.down_payment_amount = 0;
        """
        df_banderas = pd.read_sql_query(query_banderas, connection)
        str_banderas_id =  ', '.join([f"'{id}'" for id in df_banderas['id'].to_list()])
        str_banderas_inv =  ', '.join([f"'{invoices}'" for invoices in df_banderas['invoice_id'].to_list()])

        if len(df_banderas) > 0:
            query_update_banderas = f"""update payouts_factoring_invoices set factoring = false where id in ({str_banderas_id});
            update payouts_factoring_invoices_snapshot set factoring = false where invoice_id in ({str_banderas_inv});
        """ 
            cursor = connection.cursor()
            cursor.execute(query_update_banderas)
            connection.commit()
            cursor.close()
            print(f"Banderas coregidas")
        else:
            print('No hay banderas que corregir')

        print("Checks Anticipo realizados")

        # AJUSTES
        # 7. Check monto de ajustes
        query_monto_ajustes=f"""select d.payout_id, (adjustments_main_concept + students_registered_amount + students_deregistered_amount), sum(amount_adjusted)
        from payouts_factoring_details d 
        join payouts_factoring_adjustments p on d.payout_id = p.payout_id
        where p.payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}')
        group by d.payout_id, (adjustments_main_concept + students_registered_amount + students_deregistered_amount)
        having ((adjustments_main_concept + students_registered_amount + students_deregistered_amount) - sum(amount_adjusted) >1)
        or ((adjustments_main_concept + students_registered_amount + students_deregistered_amount) - sum(amount_adjusted) < -1);
        """

        df_monto_ajustes = pd.read_sql_query(query_monto_ajustes, connection)
        if len(df_monto_ajustes) > 0:
            df_monto_ajustes.to_excel(excel_writer, sheet_name='Monto_Ajustes', index=False)
            errores_por_campus[campus_id] += 1
            add_summary('Monto_Ajustes', df_monto_ajustes, conn=connection)


        #8. Check suma monto inicial y monto ajustado
        query_collection = f"""
        select pf.campus_id, pfa.payout_id, pfa.invoice_id, pfa.initial_amount_old, pfa.amount_adjusted, pfa.initial_amount, pfa.adjustment_type,
            (pfa.initial_amount_old + pfa.amount_adjusted) as suma,
            (pfa.amount_adjusted / nullif(pfa.initial_amount_old, 0)) as division,
            pfa.initial_amount - (pfa.initial_amount_old + pfa.amount_adjusted) as difference
        from payouts_factoring_adjustments pfa
        join payouts_factoring pf on pf.id = pfa.payout_id
        where pf.campus_id in ({str_campuses}) and pf.payout_date = '{payout_date}'
        and pfa.invoice_id not in (select id from invoices where status = 'deleted');
        """
        df_collection = pd.read_sql_query(query_collection, connection)

        invoices_suma_0 = df_collection[(df_collection['suma'] == 0) & (df_collection['adjustment_type'] == 'ajuste de colegiatura')]['invoice_id'].to_list()
        str_invoices_0 =  ', '.join([f"'{invoices}'" for invoices in invoices_suma_0])
        if len(invoices_suma_0) > 0:
            invoices_cambio_bandera = f""" select * from invoices where id in ({str_invoices_0})
                                        and status <> 'deleted' and initial_amount <> 0;
                                        """
            df_i_cambio_bandera = pd.read_sql_query(invoices_cambio_bandera, connection)

            if len(df_i_cambio_bandera) > 0:
                rows = []
            # Invoices que requieren update de factoring
            invoices_update = df_i_cambio_bandera[df_i_cambio_bandera['factoring'] == False]['id'].tolist()
            if invoices_update:
                str_change = ', '.join([f"'{invoice}'" for invoice in invoices_update])
                update_sql = f"UPDATE invoices SET factoring = true WHERE id IN ({str_change})"
                rows.append({
                    'tipo': 'Solicita update factoring',
                    'campus_id': campus_id,
                    'campus_name': campus_name_map.get(campus_id, ''),
                    'invoice_ids': invoices_update,
                    'update_sql': update_sql
                })
            # Invoices que ya tienen factoring = True y requieren revisión de ajustes
            invoices_revisar = df_i_cambio_bandera[df_i_cambio_bandera['factoring'] == True]['id'].tolist()
            if invoices_revisar:
                rows.append({
                    'tipo': 'Revisar ajustes',
                    'campus_id': campus_id,
                    'campus_name': campus_name_map.get(campus_id, ''),
                    'invoice_ids': invoices_revisar,
                    'update_sql': ''
                })
            # Exportar al Excel si hay filas
            if rows:
                df_cambio_bandera = pd.DataFrame(rows)
                df_cambio_bandera.to_excel(excel_writer, sheet_name='Cambio_Bandera_Factoraje', index=False)
                errores_por_campus[campus_id] += 1
                add_summary('Bandera_Mal', df_cambio_bandera, conn=connection)


        #9. Check altas falsas
        altas_falsas = df_collection[df_collection['adjustment_type'] == 'ajuste de colegiatura']['invoice_id'].to_list()
        str_altas_falsas = ', '.join([f"'{invoices}'" for invoices in altas_falsas])

        if len(altas_falsas) > 0:
            query_altas_fake = f"""
            select 
                p.campus_id, 
                c.name as campus_name, 
                s.invoice_id,  
                count(*) 
            from payouts_factoring_invoices_snapshot s
            join payouts_factoring p on p.id = s.payout_id
            join campuses c on p.campus_id = c.id
            where invoice_id in ({str_altas_falsas})
            group by p.campus_id, c.name, s.invoice_id
            having count(*) = 1;
            """

            df_altas_fake = pd.read_sql_query(query_altas_fake, connection)

            if len(df_altas_fake) > 0:
                str_altas_fake = ', '.join([f"'{inv}'" for inv in df_altas_fake['invoice_id']])
                update_sql = f"UPDATE invoices SET factoring = false WHERE id IN ({str_altas_fake})"
                update_sql_col = [update_sql] + [''] * (len(df_altas_fake) - 1)
                df_altas_fake_excel = df_altas_fake.copy()
                df_altas_fake_excel['Accionable'] = update_sql_col
                df_altas_fake_excel = df_altas_fake_excel.drop(columns=['count'])
                df_altas_fake_excel.to_excel(excel_writer, sheet_name='Altas_Falsas', index=False)
                for campus_id in df_altas_fake['campus_id']:
                    errores_por_campus[campus_id] += 1
                add_summary('Altas_Falsas', df_altas_fake_excel, conn=connection)

        print("Checks Ajustes realizados")


        #CAJAS
        #10. Check que el monto de cajas sea igual a la suma de los monto en details

        query_monto_cajas = f"""
            with selected_payouts as (
                select id, campus_id
                from payouts_factoring
                where campus_id in ({str_campuses}) and payout_date = '{payout_date}'
            ),
            suma_new as (
                select sum(amount) as suma_new, payout_id
                from payouts_factoring_cajas_new
                where payout_id in (select id from selected_payouts)
                group by payout_id
            ),
            suma_ajustes as (
                select sum(amount) as suma_adj, payout_id
                from payouts_factoring_cajas_adjustments
                where payout_id in (select id from selected_payouts)
                group by payout_id
            )
            select 
                pfd.payout_id, 
                sp.campus_id, 
                c.name as campus_name, 
                COALESCE(n.suma_new, 0) + COALESCE(a.suma_adj, 0) as suma_cajas, 
                pfd.cajas_amount
            from payouts_factoring_details pfd
                left join suma_ajustes a on a.payout_id = pfd.payout_id
                left join suma_new n on n.payout_id = pfd.payout_id
                join selected_payouts sp on sp.id = pfd.payout_id
                join campuses c on sp.campus_id = c.id
            where pfd.payout_id in ({payout_ids})
                and COALESCE(n.suma_new, 0) + COALESCE(a.suma_adj, 0) + pfd.cajas_amount <> 0;
        """

        df_monto_cajas = pd.read_sql_query(query_monto_cajas, connection)

        if len(df_monto_cajas) > 0:
            df_monto_cajas.to_excel(excel_writer, sheet_name='Monto_Cajas', index=False)
            for campus_id in df_monto_cajas['campus_id']:
                errores_por_campus[campus_id] += 1
            add_summary('Monto_Cajas', df_monto_cajas, conn=connection)


        #11. Revisar que los conceptos hayan sido anticipados
        query_cajas = f"""
            select 
                pfa.payout_id, 
                pfa.invoice_id, 
                pfa.initial_amount_old, 
                pfa.amount_adjusted, 
                pfa.initial_amount, 
                pfa.adjustment_type, 
                (pfa.initial_amount_old + pfa.amount_adjusted) as suma, 
                (pfa.amount_adjusted / nullif(pfa.initial_amount_old, 0)) as division, 
                pfa.initial_amount - (pfa.initial_amount_old + pfa.amount_adjusted) as difference,
                p.campus_id,
                c.name as campus_name
            from payouts_factoring_adjustments pfa
            join payouts_factoring p on p.id = pfa.payout_id
            join campuses c on p.campus_id = c.id
            where pfa.payout_id in ({payout_ids});
        """

        df_collection = pd.read_sql_query(query_cajas, connection)

        invoices_suma_0 = df_collection[
            (df_collection['suma'] == 0) & 
            (df_collection['adjustment_type'] == 'ajuste de colegiatura')
        ]['invoice_id'].to_list()

        str_invoices_0 = ', '.join([f"'{invoices}'" for invoices in invoices_suma_0])

        query_cajas_invoices = f"""
            select 
                pfc.payout_id, 
                pfc.invoice_id, 
                pfc.amount, 
                p.campus_id, 
                c.name as campus_name
            from payouts_factoring_cajas_new pfc
            join payouts_factoring p on p.id = pfc.payout_id
            join campuses c on p.campus_id = c.id
            where pfc.payout_id in ({payout_ids})
        """

        df_cajas_invoices = pd.read_sql_query(query_cajas_invoices, connection)
        invoices_cajas = df_cajas_invoices['invoice_id'].to_list()
        str_invoices_cajas = ', '.join([f"'{invoices}'" for invoices in invoices_cajas])

        if len(invoices_cajas) > 0:
            query_downpayment_cajas = f"""
                select 
                    p.campus_id, 
                    c.name as campus_name, 
                    pfi.invoice_id, 
                    pfi.down_payment_amount
                from payouts_factoring_invoices_snapshot pfi
                join payouts_factoring p on p.id = pfi.payout_id
                join campuses c on p.campus_id = c.id
                where pfi.invoice_id in ({str_invoices_cajas})
                and pfi.down_payment_amount <= 0;
            """

            df_downpayment_cajas = pd.read_sql_query(query_downpayment_cajas, connection)
            if len(df_downpayment_cajas) > 0:
                df_downpayment_cajas.to_excel(excel_writer, sheet_name='Cajas_Anticipadas', index=False)
                for campus_id in df_downpayment_cajas['campus_id']:
                    errores_por_campus[campus_id] += 1
                add_summary('Cajas_Anticipadas', df_downpayment_cajas, conn=connection)

        #12. Check invoices duplicadas
        query_invoices_duplicadas = f"""
            select 
                i.campus_id, 
                c.name as campus_name, 
                i.id as invoice_id, 
                i.amount as invoice_amount, 
                sum(r.amount) as total_amount, 
                i.amount - sum(r.amount) as diferencia
            from invoices i
            left join payouts_factoring_cajas_new r on i.id = r.invoice_id
            join campuses c on i.campus_id = c.id
            where i.amount > 0
            and r.payout_id in ({payout_ids})
            group by i.id, i.amount, i.campus_id, c.name
            having i.amount - sum(r.amount) < -1;
        """

        df_invoices_duplicadas = pd.read_sql_query(query_invoices_duplicadas, connection)
        invoices_duplicadas = df_invoices_duplicadas['invoice_id'].to_list()
        str_invoices_duplicadas = ', '.join([f"'{invoices}'" for invoices in invoices_duplicadas])

        if len(df_invoices_duplicadas) > 0:
            df_invoices_duplicadas.to_excel(excel_writer, sheet_name='Invoices_Infladas', index=False)
            for campus_id in df_invoices_duplicadas['campus_id']:
                errores_por_campus[campus_id] += 1
            add_summary('Invoices_Infladas', df_invoices_duplicadas, conn=connection)
    print("Checks Cajas realizados")


        #SaaS
        #13. Revisar que la suma de ingresos no anticipados sea igual al monto en details

    query_monto_saas = f"""
        WITH suma_no_anticipado AS (
            SELECT payout_id, SUM(not_anticipated_net_amount) AS suma_no_anticipado
            FROM payouts_factoring_details
            WHERE payout_id in ({payout_ids})
            GROUP BY payout_id
        ),
        suma_new AS (
            SELECT payout_id, SUM(amount) AS suma_new
            FROM payouts_saas_new_receipts
            WHERE payout_id in ({payout_ids})
            GROUP BY payout_id
        ),
        suma_ajustes AS (
            SELECT payout_id, SUM(amount) AS suma_adj
            FROM payouts_saas_adjustments
            WHERE payout_id in ({payout_ids})
            GROUP BY payout_id
        )
        SELECT
            COALESCE(n.suma_new, 0) + COALESCE(a.suma_adj, 0) + COALESCE(s.suma_no_anticipado, 0) AS total,
            n.suma_new,
            a.suma_adj,
            s.suma_no_anticipado,
            pfd.payout_id,
            p.campus_id,
            c.name as campus_name
        FROM payouts_factoring_details pfd
        LEFT JOIN suma_new n ON pfd.payout_id = n.payout_id
        LEFT JOIN suma_ajustes a ON pfd.payout_id = a.payout_id
        LEFT JOIN suma_no_anticipado s ON pfd.payout_id = s.payout_id
        JOIN payouts_factoring p ON p.id = pfd.payout_id
        JOIN campuses c ON p.campus_id = c.id
        WHERE pfd.payout_id in ({payout_ids})
        GROUP BY n.suma_new, a.suma_adj, s.suma_no_anticipado, pfd.payout_id, p.campus_id, c.name
        HAVING NOT (
            COALESCE(n.suma_new, 0) + COALESCE(a.suma_adj, 0) + COALESCE(s.suma_no_anticipado, 0)
        ) = COALESCE(s.suma_no_anticipado, 0)
    """

    df_monto_saas = pd.read_sql_query(query_monto_saas, connection)

    if len(df_monto_saas) > 0:
        df_monto_saas.to_excel(excel_writer, sheet_name='Monto_SaaS', index=False)
        for campus_id in df_monto_saas['campus_id']:
            errores_por_campus[campus_id] += 1
        add_summary('Monto_SaaS', df_monto_saas, conn=connection)

    # 14. Revisar que SaaS no se haya anticipado
        query_saas_invoices = f"""
            SELECT 
                pfc.payout_id, 
                pfc.invoice_id, 
                p.campus_id, 
                c.name as campus_name
            FROM payouts_saas_new_receipts pfc
            JOIN payouts_factoring p ON p.id = pfc.payout_id
            JOIN campuses c ON p.campus_id = c.id
            WHERE pfc.payout_id in ({payout_saas_ids})
        """

        df_saas_invoices = pd.read_sql_query(query_saas_invoices, connection)
        invoices_saas = df_saas_invoices['invoice_id'].to_list()
        str_invoices_saas = ', '.join([f"'{invoices}'" for invoices in invoices_saas])

        if len(invoices_saas) > 0:
            query_downpayment_saas = f"""
                SELECT 
                    pfi.payout_id, 
                    pfi.invoice_id, 
                    pfi.down_payment_amount, 
                    p.campus_id, 
                    c.name as campus_name
                FROM payouts_factoring_invoices_snapshot pfi
                JOIN payouts_factoring p ON p.id = pfi.payout_id
                JOIN campuses c ON p.campus_id = c.id
                WHERE pfi.invoice_id in ({str_invoices_saas})
                AND pfi.down_payment_amount > 0
            """

            df_downpayment_saas = pd.read_sql_query(query_downpayment_saas, connection)
            if len(df_downpayment_saas) > 0:
                df_downpayment_saas.to_excel(excel_writer, sheet_name='SaaS_Anticipados', index=False)
                for campus_id in df_downpayment_saas['campus_id']:
                    errores_por_campus[campus_id] += 1
                add_summary('SaaS_Anticipados', df_downpayment_saas, conn=connection)

        # 15. Revisar invoices de SaaS duplicadas
        query_saas_duplicadas = f"""
            SELECT 
                i.campus_id, 
                c.name as campus_name, 
                i.id as invoice_id, 
                i.amount as invoice_amount, 
                SUM(r.amount) as total_amount, 
                i.amount - SUM(r.amount) as diferencia
            FROM invoices i
            LEFT JOIN payouts_saas_new_receipts r ON i.id = r.invoice_id
            JOIN campuses c ON i.campus_id = c.id
            WHERE i.amount > 0
            AND r.payout_id in ({payout_saas_ids})
            GROUP BY i.id, i.amount, i.campus_id, c.name
            HAVING i.amount - SUM(r.amount) < -1
        """

        df_saas_duplicadas = pd.read_sql_query(query_saas_duplicadas, connection)
        saas_duplicadas = df_saas_duplicadas['invoice_id'].to_list()
        str_saas_duplicadas = ', '.join([f"'{invoices}'" for invoices in saas_duplicadas])

        if len(df_saas_duplicadas) > 0:
            df_saas_duplicadas.to_excel(excel_writer, sheet_name='SaaS_Infladas', index=False)
            for campus_id in df_saas_duplicadas['campus_id']:
                errores_por_campus[campus_id] += 1
            add_summary('SaaS_Infladas', df_saas_duplicadas, conn=connection)

        print("Checks SaaS realizados")

    #Checks adicionales
    #Resumen:
        #checar que el promotion coincida con el total_initial_amount + ajustes
    query_promotion= f"""SELECT
    c.name,
    pf.campus_id,
    pfd.payout_id,
    (pfd.total_initial_amount +
        pfd.adjustments_main_concept +
        pfd.students_registered_amount +
        pfd.students_deregistered_amount) * (pr.promotion_factor - pf.factoring_factor)/100 AS real_promotion,
    pfd.promotions,
    (
        ((pfd.total_initial_amount +
            pfd.adjustments_main_concept +
            pfd.students_registered_amount +
            pfd.students_deregistered_amount) * (pr.promotion_factor - pf.factoring_factor)/100)
        - pfd.promotions
    ) AS diferencia
FROM
    payouts_factoring_details pfd
LEFT JOIN payouts_factoring pf ON pfd.payout_id = pf.id
LEFT JOIN promotions pr ON pf.campus_id = pr.campus_id
JOIN campuses c ON pf.campus_id = c.id
WHERE
    pr.promotion_factor IS NOT NULL
    AND f_calculate_Date_yearmonth(pf.payout_date::date)::integer = {year_month}
    AND '{year_month}' = ANY(pr.range)
GROUP BY
    c.name,
    pf.campus_id,
    pfd.payout_id,
    pf.factoring_factor,
    pr.promotion_factor,
    pfd.promotions,
    pfd.total_initial_amount,
    pfd.adjustments_main_concept,
    pfd.students_registered_amount,
    pfd.students_deregistered_amount;
    """
    
    df_promotion = pd.read_sql_query(query_promotion, connection)
    if len(df_promotion[df_promotion['diferencia'].abs() > 1]) > 0:
        df_promotion[df_promotion['diferencia'].abs() > 1].to_excel(excel_writer, sheet_name='Promotion', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Promotion', df_promotion[df_promotion['diferencia'].abs() > 1], conn=connection)

#Anticipo:
    # checar que la suma de anticipado cuadre contra el total_initial_amount
    query_initial_amount=f"""with sum_invoices as (select payout_id, sum(initial_amount) as sum_initial_amount
                        from payouts_factoring_invoices
                        where factoring = true
                        group by payout_id)
        select pf.campus_id,
            c.name as campus_name,
            si.payout_id,
            si.sum_initial_amount,
            pfi.total_initial_amount,
            si.sum_initial_amount - pfi.total_initial_amount as diferencias
        from payouts_factoring_details pfi
                left join sum_invoices si on pfi.payout_id = si.payout_id
                left join payouts_factoring pf on pfi.payout_id = pf.id
                left join campuses c on pf.campus_id = c.id
        where (si.sum_initial_amount - pfi.total_initial_amount > 1 or si.sum_initial_amount - pfi.total_initial_amount < -1)
        and pfi.payout_id in (
        select id
        from payouts_factoring
        where campus_id in ({str_campuses})
        and f_calculate_date_yearmonth(payout_date::date) = '{year_month}'
                                );
                                """
    df_initial_amount = pd.read_sql_query(query_initial_amount, connection)
    if len(df_initial_amount) > 0:
        df_initial_amount.to_excel(excel_writer, sheet_name='Initial_Amount', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Initial_Amount', df_initial_amount, conn=connection)

    #checar que se anticipe lo que se debe anticipar
    query_anticipable=f"""select pfi.payout_id,
        pf.campus_id,
        c.name as campus_name,
        string_agg(distinct pfi.concept_type, ',' order by pfi.concept_type)                                  as conceptos_anticipados,
        case
            when (c.memberships_factoring and c.inscriptions_factoring and c.complements_factoring)
                then 'complement, inscription, membership'
            when (c.memberships_factoring and c.inscriptions_factoring) then 'inscription, membership'
            when (c.memberships_factoring and c.complements_factoring) then 'complement, membership'
            when (c.inscriptions_factoring and c.complements_factoring) then 'complement, inscription'
            when (c.memberships_factoring) then 'membership'
            when (c.complements_factoring) then 'complement'
            when (c.inscriptions_factoring)
                then 'inscription' end                                                                        as tipo_concepto_anticipable,
        string_agg(distinct pfi.concept_type, ', ' order by pfi.concept_type) = case
                                                                                    when (c.memberships_factoring and
                                                                                            c.inscriptions_factoring and
                                                                                            c.complements_factoring)
                                                                                        then 'complement, inscription, membership'
                                                                                    when (c.memberships_factoring and c.inscriptions_factoring)
                                                                                        then 'inscription, membership'
                                                                                    when (c.memberships_factoring and c.complements_factoring)
                                                                                        then 'complement, membership'
                                                                                    when (c.inscriptions_factoring and c.complements_factoring)
                                                                                        then 'complement, inscription'
                                                                                    when (c.memberships_factoring)
                                                                                        then 'membership'
                                                                                    when (c.complements_factoring)
                                                                                        then 'complement'
                                                                                    when (c.inscriptions_factoring)
                                                                                        then 'inscription' end as diferencia
    from payouts_factoring_invoices pfi
            left join payouts_factoring pf on pfi.payout_id = pf.id
            left join campuses c on pf.campus_id = c.id
    where pfi.factoring = true
    and pfi.payout_id in (select id
    from payouts_factoring
    where campus_id in ({str_campuses})
    and f_calculate_date_yearmonth(payout_date::date) = '{year_month}')
    group by pfi.payout_id,
            case
                when (c.memberships_factoring and c.inscriptions_factoring and c.complements_factoring)
                    then 'complement, inscription, membership'
                when (c.memberships_factoring and c.inscriptions_factoring) then 'inscription, membership'
                when (c.memberships_factoring and c.complements_factoring) then 'complement, membership'
                when (c.inscriptions_factoring and c.complements_factoring) then 'complement, inscription'
                when (c.memberships_factoring) then 'membership'
                when (c.complements_factoring) then 'complement'
                when (c.inscriptions_factoring) then 'inscription' end,
        pf.campus_id,
        c.name;
        """
    df_anticipable = pd.read_sql_query(query_anticipable, connection)
    if len(df_anticipable[df_anticipable['diferencia'] == False]) > 0:
        df_anticipable[df_anticipable['diferencia'] == False].to_excel(excel_writer, sheet_name='Conceptos_Anticipables', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Conceptos_Anticipables', df_anticipable[df_anticipable['diferencia'] == False], conn=connection)

   
    #checar que no se esté anticipando un invoice con monto negativo
    query_invoices_negativas=f"""    select c.name as campus_name, pfi.*
        from payouts_factoring_invoices pfi
                left join payouts_factoring pf on pfi.payout_id = pf.id
                left join campuses c on pf.campus_id = c.id
        where payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}')
        and initial_amount < 0;
        """
    df_invoices_negativas = pd.read_sql_query(query_invoices_negativas, connection)
    if len(df_invoices_negativas) > 0:
        df_invoices_negativas.to_excel(excel_writer, sheet_name='Invoices_Negativas', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Invoices_Negativas', df_invoices_negativas, conn=connection)

    #Ajustes
        #checar que no haya ajustes por un monto superior al initial_amount
    query_ajustes_initial_amount=f"""    select c.name, pfa.*
        from payouts_factoring_adjustments pfa
                left join payouts_factoring pf on pfa.payout_id = pf.id
                left join campuses c on c.id = pf.campus_id
        where payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}')
        and amount_adjusted > initial_amount
        and initial_amount_old <> 0;
        """
    df_ajustes_initial_amount = pd.read_sql_query(query_ajustes_initial_amount, connection)
    if len(df_ajustes_initial_amount) > 0:
        df_ajustes_initial_amount.to_excel(excel_writer, sheet_name='Ajustes_InitialAmount', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Ajustes_InitialAmount', df_ajustes_initial_amount, conn=connection)

    # checar que no haya 2 ajustes para 1 sola invoice en el mismo payout
    query_ajustes_dobles=f"""   select c.name as campus_name, pf.campus_id, pfa.payout_id, pfa.invoice_id, count(*)
        from payouts_factoring_adjustments pfa
                left join payouts_factoring pf on pf.id = pfa.payout_id
                left join campuses c on c.id = pf.campus_id
        where payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}')
        group by c.name, pf.campus_id, pfa.payout_id ,pfa.invoice_id
        having count(*) > 1;
        """
    df_ajustes_dobles = pd.read_sql_query(query_ajustes_dobles, connection)
    if len(df_ajustes_dobles) > 0:
        df_ajustes_dobles.to_excel(excel_writer, sheet_name='Ajustes_Dobles', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Ajustes_Dobles', df_ajustes_dobles, conn=connection)

    # checar que la suma de ajustes por separado coincida con details
    query_ajustes_details=f"""with sum_adjustments as (    select
        payout_id,
        sum(case when adjustment_type = 'ajuste de colegiatura' then amount_adjusted else 0 end) as sum_adjustment_main_concept,
        sum(case when adjustment_type = 'baja' then amount_adjusted else 0 end) as sum_students_deregistered_amount,
        sum(case when adjustment_type = 'alta' then amount_adjusted else 0 end) as sum_students_registered_amount
        from payouts_factoring_adjustments
        group by payout_id)
        select
            c.name as campus_name,
            pfd.payout_id,
            -- columnas originales
            pfd.adjustments_main_concept,
            pfd.students_deregistered_amount,
            pfd.students_registered_amount,
            -- sumas desde ajustes
            sa.sum_adjustment_main_concept,
            sa.sum_students_deregistered_amount,
            sa.sum_students_registered_amount,
            -- diferencias
            (pfd.adjustments_main_concept - sa.sum_adjustment_main_concept) as diff_adjustment_main_concept,
            (pfd.students_deregistered_amount - sa.sum_students_deregistered_amount) as diff_students_deregistered_amount,
            (pfd.students_registered_amount - sa.sum_students_registered_amount) as diff_students_registered_amount
        from payouts_factoring_details pfd
        left join sum_adjustments sa
            on pfd.payout_id = sa.payout_id
        left join payouts_factoring pf
            on pf.id = pfd.payout_id
        left join campuses c
            on c.id = pf.campus_id
        where pfd.payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}');
        """
    df_ajustes_details = pd.read_sql_query(query_ajustes_details, connection)
    if len(df_ajustes_details[(df_ajustes_details['diff_adjustment_main_concept'].abs() > 1) |
                                (df_ajustes_details['diff_students_deregistered_amount'].abs() > 1) |
                                (df_ajustes_details['diff_students_registered_amount'].abs() > 1)]) > 0:
            df_ajustes_details[(df_ajustes_details['diff_adjustment_main_concept'].abs() > 1) |
                            (df_ajustes_details['diff_students_deregistered_amount'].abs() > 1) |
                            (df_ajustes_details['diff_students_registered_amount'].abs() > 1)].to_excel(excel_writer, sheet_name='Ajustes_vs_Details', index=False)
            errores_por_campus[campus_id] += 1
            add_summary('Ajustes_vs_Details', df_ajustes_details[(df_ajustes_details['diff_adjustment_main_concept'].abs() > 1) |
                                                                (df_ajustes_details['diff_students_deregistered_amount'].abs() > 1) |
                                                                (df_ajustes_details['diff_students_registered_amount'].abs() > 1)], conn=connection)
    
    # checar que las altas correspondan a conceptos anticipables
    query_altas_anticipables=f"""    select
            pfa.id as adjustment_id,
            pfa.payout_id,
            pf.campus_id,
            c.name as campus_name,
            pfa.concept_type,
            pfa.amount_adjusted,
            c.memberships_factoring,
            c.complements_factoring,
            c.inscriptions_factoring
        from payouts_factoring_adjustments pfa
        join payouts_factoring pf
            on pfa.payout_id = pf.id
        join campuses c
            on pf.campus_id = c.id
        where pfa.adjustment_type = 'alta'
        and not (
                (pfa.concept_type = 'membership' and c.memberships_factoring = true)
            or (pfa.concept_type = 'complement' and c.complements_factoring = true)
            or (pfa.concept_type = 'inscription' and c.inscriptions_factoring = true)
        )
        and pfa.payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}');     
        """   
    df_altas_anticipables = pd.read_sql_query(query_altas_anticipables, connection)
    if len(df_altas_anticipables) > 0:
        df_altas_anticipables.to_excel(excel_writer, sheet_name='Altas_No_Anticipables', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Altas_No_Anticipables', df_altas_anticipables, conn=connection)    

    # checar que no haya ajustes sobre conceptos pre-mattilda.
    query_premattilda=f"""  select pfa.payout_id,
            pf.campus_id,
            c.name,
            pfa.id as adjustment_id,
            pfa.invoice_id,
            pfa.date_period,
            pfa.adjustment_type,
            pf.payout_date
        from payouts_factoring_adjustments pfa
                left join payouts_factoring pf on pfa.payout_id = pf.id
                left join campuses c on pf.campus_id = c.id
            and f_calculate_date_yearmonth(pfa.date_period) < f_calculate_date_yearmonth(c.operations_start_date)
            and payout_id in (select id from payouts_factoring where campus_id in ({str_campuses}) and payout_date='{payout_date}');
            """
    df_pre_mattilda = pd.read_sql_query(query_premattilda, connection)
    if len(df_pre_mattilda) > 0:
        df_pre_mattilda.to_excel(excel_writer, sheet_name='Ajustes_Pre-Mattilda', index=False)
        errores_por_campus[campus_id] += 1
        add_summary('Ajustes_Pre-Mattilda', df_pre_mattilda, conn=connection)

    print("Checks Adicionales realizados")


    # ============== RESUMEN FINAL: checks_anticipo ==================
    
    ddf_sum = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame([{'check': 'Sin_checks', 'campus_id': c, 'coincidencias': 0} for c in campus_ids])
    ddf_sum['campus_name'] = ddf_sum['campus_id'].map(campus_name_map)
    pivot = ddf_sum.pivot_table(index=['campus_id', 'campus_name'], columns='check', values='coincidencias', aggfunc='sum', fill_value=0)
    pivot['total'] = pivot.sum(axis=1)

    # Identificar los campus listos
    pivot['campus_listo'] = pivot['total'] == 0

    # Identificar los campus con solo fallos en el check de anualidades
    if 'Anualidades' in pivot.columns:
        pivot['Corregir_Anualiadades'] = (pivot['total'] == pivot['Anualidades']) & (pivot['Anualidades'] > 0)
    else:
        pivot['Corregir_Anualidades'] = False

    pivot = pivot.reset_index()
    pivot.to_excel(excel_writer, sheet_name='checks_anticipo', index=False)
    print("Se concluyeron todos los checks para todos los campus \nEl excel fue descargado con éxito")



# Cerrar la conexión a la base de datos
if connection:
    connection.close()
    print("Conexión a PostgreSQL cerrada")

