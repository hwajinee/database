import pandas as pd
from db_conn import *


def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    df = pd.read_excel(excel_file, skiprows=4)

    # 영화 테이블 생성 SQL
    create_movie_table_sql = """
        DROP TABLE IF EXISTS movie, director, genre, movie_director, movie_genre;
        CREATE TABLE movie (
            m_id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(500),
            eng_title VARCHAR(500),
            year INT,
            country VARCHAR(100),
            m_type VARCHAR(10),
            status VARCHAR(30),
            company VARCHAR(100)
        );
    """

    # 감독 테이블 생성 SQL
    create_director_table_sql = """
        CREATE TABLE director (
            d_id INT AUTO_INCREMENT PRIMARY KEY,
            d_name VARCHAR(100) 
        );
    """

    # 장르 테이블 생성 SQL
    create_genre_table_sql = """
        CREATE TABLE genre (
            genre_id INT AUTO_INCREMENT PRIMARY KEY,
            genre_name VARCHAR(100) 
        );
    """

    # 영화-감독 매핑 테이블 생성 SQL
    create_movie_director_table_sql = """
        CREATE TABLE movie_director (
            m_id INT,
            d_id INT,
            PRIMARY KEY (m_id, d_id),
            FOREIGN KEY (m_id) REFERENCES movie(m_id),
            FOREIGN KEY (d_id) REFERENCES director(d_id)
        );
    """

    # 영화-장르 매핑 테이블 생성 SQL
    create_movie_genre_table_sql = """
        CREATE TABLE movie_genre (
            m_id INT,
            genre_id INT,
            PRIMARY KEY (m_id, genre_id),
            FOREIGN KEY (m_id) REFERENCES movie(m_id),
            FOREIGN KEY (genre_id) REFERENCES genre(genre_id)
        );
    """

    # 테이블 생성 SQL 실행
    cur.execute(create_movie_table_sql)
    cur.execute(create_director_table_sql)
    cur.execute(create_genre_table_sql)
    cur.execute(create_movie_director_table_sql)
    cur.execute(create_movie_genre_table_sql)
    conn.commit()

    # 감독 및 장르 정보를 저장하기 위한 딕셔너리 생성
    director_ids = {}
    next_director_id = 1

    genre_ids = {}
    next_genre_id = 1

    # 감독 정보 삽입 SQL
    insert_director_sql = "INSERT INTO director (d_name) VALUES (%s);"

    # 장르 정보 삽입 SQL
    insert_genre_sql = "INSERT INTO genre (genre_name) VALUES (%s);"

    # 데이터프레임에서 감독 및 장르 정보 처리
    for index, row in df.iterrows():
        try:
            d_name = row['감독']
            if pd.notna(d_name) and d_name not in director_ids:
                try:
                    cur.execute(insert_director_sql, (d_name,))
                    director_ids[d_name] = next_director_id
                    next_director_id += 1
                except pymysql.IntegrityError as e:
                    if e.args[0] == 1062:  # Duplicate entry error code
                        pass  # 이미 존재하는 감독이므로 넘어감
        except KeyError:
            pass

        try:
            genre_name = row['장르']
            if pd.notna(genre_name) and genre_name not in genre_ids:
                cur.execute(insert_genre_sql, (genre_name,))
                genre_ids[genre_name] = next_genre_id
                next_genre_id += 1
        except KeyError:
            pass

    conn.commit()

    # 영화 정보 삽입 SQL
    insert_movie_sql = """
        INSERT INTO movie (title, eng_title, year, country, m_type, status, company)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    # 영화-감독 매핑 정보 삽입 SQL
    insert_movie_director_sql = """
        INSERT INTO movie_director (m_id, d_id)
        VALUES (%s, %s);
    """

    # 영화-장르 매핑 정보 삽입 SQL
    insert_movie_genre_sql = """
        INSERT INTO movie_genre (m_id, genre_id)
        VALUES (%s, %s);
    """

    movie_count = 0
    director_count = len(director_ids)
    genre_count = len(genre_ids)

    # 데이터프레임에서 영화 정보 처리
    for index, row in df.iterrows():
        try:
            title = row['영화명']
            if pd.notna(title):
                # 영화 정보 삽입
                cur.execute(insert_movie_sql, (
                    title,
                    row.get('영화명(영문)', '없음'),
                    row.get('제작연도', '없음'),
                    row.get('제작국가', '없음'),
                    row.get('유형', '없음'),
                    row.get('제작상태', '없음'),
                    row.get('제작사', '없음')
                ))
                movie_id = cur.lastrowid
                movie_count += 1

                # 감독 및 장르 ID 조회 및 매핑 정보 삽입
                try:
                    d_name = row['감독']
                    if pd.notna(d_name) and d_name in director_ids:
                        director_id = director_ids[d_name]
                        cur.execute(insert_movie_director_sql, (movie_id, director_id))
                except KeyError:
                    pass

                try:
                    genre_name = row['장르']
                    if pd.notna(genre_name) and genre_name in genre_ids:
                        genre_id = genre_ids[genre_name]
                        cur.execute(insert_movie_genre_sql, (movie_id, genre_id))
                except KeyError:
                    pass
        except KeyError:
            pass

    conn.commit()

    close_db(conn, cur)

    # 결과 출력
    print(f"Inserted {movie_count} movies into 'movie' table.")
    print(f"Inserted {director_count} directors into 'director' table.")
    print(f"Inserted {genre_count} genres into 'genre' table.")


if __name__ == '__main__':
    read_excel_into_mysql()
