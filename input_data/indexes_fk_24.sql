create index fk_person_role_id_cast_info on cast_info(person_role_id);
create index fk_role_id_cast_info on cast_info(role_id);
create index fk_subject_id_complete_cast on complete_cast(subject_id);
create index fk_status_id_complete_cast on complete_cast(status_id);
create index fk_linked_movie_id_movie_link on movie_link(linked_movie_id);
create index fk_link_type_id_movie_link on movie_link(link_type_id);
create index fk_company_id_movie_companies on movie_companies(company_id);
create index fk_company_type_id_movie_companies on movie_companies(company_type_id);
create index fk_kind_id_title on title(kind_id);
create index fk_keyword_id_movie_keyword on movie_keyword(keyword_id);

create index fk_person_id_cast_info on cast_info(person_id);
create index fk_person_id_person_info on person_info(person_id);
create index fk_person_id_aka_name on aka_name(person_id);

create index fk_info_type_id_movie_info on movie_info(info_type_id);
create index fk_info_type_id_movie_info_idx on movie_info_idx(info_type_id);
create index fk_info_type_id_person_info on person_info(info_type_id);

create index fk_movie_id_cast_info on cast_info(movie_id);
create index fk_movie_id_complete_cast on complete_cast(movie_id);
create index fk_movie_id_movie_link on movie_link(movie_id);
create index fk_movie_id_movie_companies on movie_companies(movie_id);
create index fk_movie_id_movie_keyword on movie_keyword(movie_id);
create index fk_movie_id_aka_title on aka_title(movie_id);
create index fk_movie_id_movie_info on movie_info(movie_id);
create index fk_movie_id_movie_info_idx on movie_info_idx(movie_id);

