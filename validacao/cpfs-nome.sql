# filtro: CPF>0
# filtro: tipo=="STRING"
select count(b.cpf)*1.0/count(1) 
from {tabela} a
left join cpf b on b.cpf=a.{coluna_1} and b.name=a.{coluna_2}