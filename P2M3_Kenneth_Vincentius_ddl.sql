-- membuat table segment
create table table_M3
(
	age int,
	sex int,
	cp int,
	trtbps int,
	chol int,
	fbs int,
	restecg int,
	thalachh int,
	exng int,
	oldpeak numeric,
	slp int,
	caa int,
	thall int,
	output int
);

--mengisi detail data dari csv untuk mengisi tabel
COPY table_M3(age,sex,cp,trtbps,chol,fbs,restecg,thalachh,exng,oldpeak,slp,caa,thall,output)
FROM 'C:\Users\kenne\Pribadi\Hackt8\Phase 2\milestone\dags\heart.csv'
DELIMITER ','
CSV HEADER;	

--memanggil data tabel yang sudah terisi untuk disimpan ke dalam bentuk file csv
select * from table_M3