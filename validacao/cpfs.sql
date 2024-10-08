# filtro: CPF>0
select count(b.cpf)*1.0/count(a.{coluna}) 
from {tabela} a
left join cpf b on b.cpf=a.{coluna}