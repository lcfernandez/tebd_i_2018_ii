# Luiz Cláudio Frederico Fernandez, M.Sc.

# Parágrafo 1
## Importações necessárias para a execução do trabalho
```
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("Trabalho TEBD - I").getOrCreate()
import spark.implicits._
import org.apache.spark.sql.types._
```

# Parágrafo 2
## Criação dos esquemas para os dataframes
```
val esquemaServicos = new StructType().add("codigo", IntegerType).add("descricao", StringType).add("tipo", StringType)
val esquemaInsumos = new StructType().add("codigo", IntegerType).add("descricao", StringType).add("unidade", StringType).add("coeficiente", FloatType).add("codigoServico", IntegerType).add("base", StringType)
```

# Parágrafo 3
## Leitura dos .csv para geração dos dataframes de serviços e insumos
## ATENÇÃO: trocar o `/path/to` para o caminho escolhido para armazenar os arquivos
```
val servicosDF = spark.read.schema(esquemaServicos).csv("/path/to/servicos/*.csv").dropDuplicates()
val insumosDF = spark.read.schema(esquemaInsumos).csv("/path/to/insumos/*.csv")
```

# Parágrafo 4
## Geração de dataframe de tipos através do de serviços
```
val tiposDF = servicosDF.map(x => (x(2).asInstanceOf[String])).dropDuplicates()
```

# Parágrafo 5
## Confecção de combo box para escolha do tipo de serviço e seus resultados no Zeppelin
```
var tipos = tiposDF.collect.map(x => (x.asInstanceOf[String], x.asInstanceOf[String]))
val filtroTipo = z.select("Tipo", tipos).asInstanceOf[String]
z.show(servicosDF.filter($"tipo" === filtroTipo))
```

# Parágrafo 6
## Confecção de combo box para escolha do serviço (filtrado pelo tipo) e seus resultados no Zeppelin
```
var servicosTipo = servicosDF.filter($"tipo" === filtroTipo).collect.map(x => (x(0).asInstanceOf[Integer], x(1).asInstanceOf[String]))
val filtroServico = z.select("Tipo", servicosTipo).asInstanceOf[Double]
z.show(insumosDF.filter($"codigoServico" === filtroServico))
```

# Parágrafo 7
## Cópia do resultado anterior para outro dataframe
```
val insumosServicoDF = insumosDF.filter($"codigoServico" === filtroServico)
```

## Confecção de combo box para escolha da base (filtrado pelo serviço) e seus resultados no Zeppelin
```
var servicosBase = insumosServicoDF.dropDuplicates("base").collect.map(x => (x(5).asInstanceOf[String], x(5).asInstanceOf[String]))
val filtroBase = z.select("Base", servicosBase).asInstanceOf[String]
z.show(insumosServicoDF.filter($"base" === filtroBase).groupBy("codigo","descricao","unidade").agg(avg("coeficiente") as "coeficiente"))
```