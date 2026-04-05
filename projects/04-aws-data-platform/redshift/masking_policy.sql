-- Executar no Redshift apos criar a tabela golden_customers
-- Configura Dynamic Data Masking pra CPF por perfil

-- 1. Criar masking policy
CREATE MASKING POLICY mask_cpf
  WITH (cpf VARCHAR)
  USING ('***' || SUBSTRING(cpf, 4, 6) || '**');

-- 2. Criar roles
CREATE ROLE role_bi;
CREATE ROLE role_gerentes;

-- 3. Aplicar masking: BI ve mascarado, gerentes veem real
ATTACH MASKING POLICY mask_cpf
  ON golden_customers(cpf)
  TO ROLE role_bi;

-- Gerentes: sem masking policy = veem o valor real

-- 4. Criar users e atribuir roles (senhas sao exemplo — trocar em producao)
CREATE USER user_bi PASSWORD '<TROCAR>';
CREATE USER user_gerente PASSWORD '<TROCAR>';

GRANT ROLE role_bi TO user_bi;
GRANT ROLE role_gerentes TO user_gerente;

-- 5. Grants de acesso
GRANT USAGE ON SCHEMA public TO ROLE role_bi;
GRANT USAGE ON SCHEMA public TO ROLE role_gerentes;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE role_bi;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ROLE role_gerentes;

-- Teste:
-- SET SESSION AUTHORIZATION user_bi;
-- SELECT golden_id, name, cpf FROM golden_customers LIMIT 5;
-- Resultado: cpf = ***456789**
--
-- SET SESSION AUTHORIZATION user_gerente;
-- SELECT golden_id, name, cpf FROM golden_customers LIMIT 5;
-- Resultado: cpf = 12345678901
