# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

tab1, tab2 = st.tabs(["Reporte Comunidad", "Reporte Individual"])

with tab1:
    # Write directly to the app
    st.title("Reporte Actividad Comunidad")
    st.write(
        """Este reporte brinda información sobre la participación en 
        sesiones, inscripción y finalización de cursos de los miembros 
        la comunidad.
        """
    )

    def toggle_dataframe_visibility(button_text, session_state_key, dataframe, key=None):
        if session_state_key not in st.session_state:
            st.session_state[session_state_key] = False

        if st.button(button_text, key=key):
            st.session_state[session_state_key] = not st.session_state[session_state_key]

        if st.session_state[session_state_key]:
            st.write(dataframe)

    # Get the current credentials
    session = get_active_session()

    # Execute SQL query and get results
    result = session.sql("SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")

    # Convert Snowpark DataFrame to pandas DataFrame
    df = result.to_pandas()

    toggle_dataframe_visibility('Mostrar/Ocultar Listado Completo Comunidad', 'show_comunidad_df', df, key='comunidad_button')

    # Option to search session or course information
    st.write('¿Deseas buscar información de una sesión o curso?')
    option = st.selectbox('Selecciona una opción:', ['Sesión', 'Curso', ''])

    # Fetch session or course information based on the selected option
    if option == 'Sesión':
        # Query for session information
        session_result = session.sql("SELECT NOMBRE_SESION FROM LABORATORIO.MONICA_SOBERON.SESION;")
        session_df = session_result.to_pandas()
        session_names = session_df['NOMBRE_SESION'].tolist()

        # Display session select box
        selected_session = st.selectbox('Selecciona una Sesión:', session_names)
        if selected_session:
            # Query for session details based on the selected session
            session_details_result = session.sql(f"SELECT NOMBRE_SESION, FECHA_SESION, LINK_SESION_INFORMATIVA FROM LABORATORIO.MONICA_SOBERON.SESION WHERE NOMBRE_SESION = '{selected_session}';")

            # Convert to pandas DataFrame
            session_details_df = session_details_result.to_pandas()

            # Display the session details as a list
            st.write("**Detalles de la Sesión:**")
            for index, row in session_details_df.iterrows():
                st.write(f" Nombre de la Sesión: {row['NOMBRE_SESION']}")
                st.write(f" Fecha de la Sesión: {row['FECHA_SESION']}")
                st.write(f" Link Informativa: {row['LINK_SESION_INFORMATIVA']}")

        invitados_counts = session.sql("SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.SESION AS S ON I.ID_SESION = S.ID_SESION;")
        invitado_count = invitados_counts.collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invitado_count}")

        invitadosSesion = session.sql("SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.SESION AS S ON I.ID_SESION = S.ID_SESION;")
        invitados_sesion_df = invitadosSesion.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado Invitados', 'show_invitados_df', invitados_sesion_df, key='invitados_button')

        asistios_counts = session.sql("SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.SESION AS S ON I.ID_SESION = S.ID_SESION;")
        asistio_count = asistios_counts.collect()[0][0]
        st.write(f" **Cantidad de Usuarios que Asistieron:** {asistio_count}")

        asistioSesion = session.sql("SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.SESION AS S ON I.ID_SESION = S.ID_SESION;")
        asistio_sesion_df = asistioSesion.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios que Asistieron', 'show_asistio_df', asistio_sesion_df, key='asistio_button')

        data = {'Status': ['Invited', 'Attended'], 'Count': [invitado_count, asistio_count]}
        df = pd.DataFrame(data)

    elif option == 'Curso':
        # Query for course information
        course_result = session.sql("SELECT NOMBRE_CURSO FROM LABORATORIO.MONICA_SOBERON.CURSO;")
        course_df = course_result.to_pandas()
        course_names = course_df['NOMBRE_CURSO'].tolist()

        # Display course select box
        selected_course = st.selectbox('Selecciona un Curso:', course_names)
        if selected_course:
            # Query for course details based on the selected course
            course_details_result = session.sql(f"SELECT NOMBRE_CURSO, FECHA_INICIO, FECHA_FIN, PROVEEDOR, NOMBRE_INSTRUCTOR, CORREO_INSTRUCTOR, REQUIERE_CASO_USO FROM LABORATORIO.MONICA_SOBERON.CURSO WHERE NOMBRE_CURSO = '{selected_course}';")

            # Convert to pandas DataFrame
            course_details_df = course_details_result.to_pandas()

            # Display the course details as a list
            st.write("**Detalles del Curso:**")
            for index, row in course_details_df.iterrows():
                st.write(f" Nombre del Curso: {row['NOMBRE_CURSO']}")
                st.write(f" Fecha de Inicio: {row['FECHA_INICIO']}")
                st.write(f" Fecha de Fin: {row['FECHA_FIN']}")
                st.write(f" Proveedor: {row['PROVEEDOR']}")
                st.write(f" Nombre Instructor: {row['NOMBRE_INSTRUCTOR']}")
                st.write(f" Correo Instructor: {row['CORREO_INSTRUCTOR']}")
                if row['REQUIERE_CASO_USO']:
                    caso = 'Si'
                else:
                    caso = 'No'
                st.write(f" Requiere Caso de Uso: {caso}")

        invitados_c = session.sql("SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS S ON I.ID_CURSO = S.ID_CURSO;")
        invitado_c = invitados_c.collect()[0][0]
        st.write(f"**Cantidad de Invitados:** {invitado_c}")

        invitadosSesionC = session.sql("SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.INVITACION_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS S ON I.ID_CURSO = S.ID_CURSO;")
        invitados_sesionC_df = invitadosSesionC.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Listado Invitados', 'show_invitados_df', invitados_sesionC_df, key='invitados_button')

        registros_c = session.sql("SELECT COUNT(*) FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS S ON I.ID_CURSO = S.ID_CURSO;")
        registro_c = registros_c.collect()[0][0]
        st.write(f"**Cantidad de Usuarios Registrados:** {registro_c}")

        registroSesionC = session.sql("SELECT NOMBRE, APELLIDO, CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD AS C INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS I ON C.ID_USUARIO = I.ID_USUARIO INNER JOIN LABORATORIO.MONICA_SOBERON.CURSO AS S ON I.ID_CURSO = S.ID_CURSO;")
        registro_sesionC_df = registroSesionC.to_pandas()
        toggle_dataframe_visibility('Mostrar/Ocultar Usuarios que Asistieron', 'show_asistio_df', registro_sesionC_df, key='asistio_button')


with tab2:
    st.title("Reporte Actividad Individual")
    st.write(
        """Este reporte permite consultar la actividad de los miembros de la 
        comunidad de forma individualizada, incluyendo sesiones y cursos en 
        los que han participado.
        """
    )

    # Query to get a list of community members
    community_result = session.sql("SELECT CORREO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD;")
    community_df = community_result.to_pandas()
    community_emails = community_df['CORREO'].tolist()

    # Display selectbox for individual member selection
    selected_member = st.selectbox('Selecciona un miembro:', community_emails)
    if selected_member:
        # Query to get individual member details
        member_result = session.sql(f"SELECT NOMBRE, APELLIDO, CORREO, STATUS FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}';")
        member_df = member_result.to_pandas()

        # Display member details
        st.write("**Detalles del Miembro:**")
        for index, row in member_df.iterrows():
            st.write(f" Nombre: {row['NOMBRE']}")
            st.write(f" Apellido: {row['APELLIDO']}")
            st.write(f" Correo: {row['CORREO']}")
            st.write(f" Estatus: {row['STATUS']}")

        # Query to get sessions the member has attended
        member_sessions_result = session.sql(f"SELECT S.NOMBRE_SESION, S.FECHA_SESION FROM LABORATORIO.MONICA_SOBERON.SESION AS S INNER JOIN LABORATORIO.MONICA_SOBERON.ASISTENCIA_SESION AS A ON S.ID_SESION = A.ID_SESION WHERE A.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');")
        member_sessions_df = member_sessions_result.to_pandas()

        # Display session details for the member
        st.write("**Sesiones Asistidas por el Miembro:**")
        st.write(member_sessions_df)

        # Query to get courses the member has attended
        member_courses_result = session.sql(f"SELECT C.NOMBRE_CURSO, C.FECHA_INICIO, C.FECHA_FIN, R.SOLICITUD_APROBADA, R.CURSO_APROBADO FROM LABORATORIO.MONICA_SOBERON.CURSO AS C INNER JOIN LABORATORIO.MONICA_SOBERON.REGISTRADOS_CURSO AS R ON C.ID_CURSO = R.ID_CURSO WHERE R.ID_USUARIO = (SELECT ID_USUARIO FROM LABORATORIO.MONICA_SOBERON.COMUNIDAD WHERE CORREO = '{selected_member}');")
        member_courses_df = member_courses_result.to_pandas()

        # Display course details for the member
        st.write("**Cursos Inscritos por el Miembro:**")
        st.write(member_courses_df)

    
