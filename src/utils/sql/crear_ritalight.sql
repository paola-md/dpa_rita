-- Crea una tabla con menos renglones que rita.raw para disminuir
-- tiempo de procesamiento en la entrega

CREATE TABLE rita.raw_light AS
  SELECT *
  FROM rita.raw
  LIMIT 1000;
