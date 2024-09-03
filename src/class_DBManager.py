import psycopg2


class DBManager:
    def __init__(self, host, database, user, password):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
        except (Exception, psycopg2.Error) as error:
            print("Error connecting to the database:", error)

    def get_companies_and_vacancies_count(self):
        try:
            self.connect()
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.name, COUNT(v.id) AS vacancies_count
                    FROM companies c
                    LEFT JOIN vacancies v ON c.id = v.company_id
                    GROUP BY c.name
                """)
                return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error executing the query:", error)
        finally:
            if self.conn:
                self.conn.close()

    def get_all_vacancies(self):
        try:
            self.connect()
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.name AS company_name, v.title, v.salary, v.url
                    FROM vacancies v
                    JOIN companies c ON v.company_id = c.id
                """)
                return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error executing the query:", error)
        finally:
            if self.conn:
                self.conn.close()

    def get_avg_salary(self):
        try:
            self.connect()
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT AVG(salary) AS avg_salary
                    FROM vacancies
                """)
                return cursor.fetchone()[0]
        except (Exception, psycopg2.Error) as error:
            print("Error executing the query:", error)
        finally:
            if self.conn:
                self.conn.close()

    def get_vacancies_with_higher_salary(self):
        try:
            self.connect()
            avg_salary = self.get_avg_salary()
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.name AS company_name, v.title, v.salary, v.url
                    FROM vacancies v
                    JOIN companies c ON v.company_id = c.id
                    WHERE v.salary > %s
                """, (avg_salary,))
                return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error executing the query:", error)
        finally:
            if self.conn:
                self.conn.close()

    def get_vacancies_with_keyword(self, keyword):
        try:
            self.connect()
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT c.name AS company_name, v.title, v.salary, v.url
                    FROM vacancies v
                    JOIN companies c ON v.company_id = c.id
                    WHERE v.title ILIKE %s
                """, (f'%{keyword}%',))
                return cursor.fetchall()
        except (Exception, psycopg2.Error) as error:
            print("Error executing the query:", error)
        finally:
            if self.conn:
                self.conn.close()
