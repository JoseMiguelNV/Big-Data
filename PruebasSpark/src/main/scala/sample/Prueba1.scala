package sample

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.DecimalType
import org.scalatest.funsuite.AnyFunSuite

class Prueba1 extends AnyFunSuite {

//  test("prueba1"){
//    val suma = 5 + 5
//    assert(10.equals(suma))
//  }
  val ss = SparkSession
    .builder()
    .appName("Stage4")
    .config("spark.master", "local[*]")
    .getOrCreate()

  test("READ Stage4 IRB") {
    val path: String = "C:\\Users\\josem\\OneDrive\\Escritorio\\PruebasSpark\\parquetPrueba\\part-00000-18800b2e-2f80-4566-915c-a345a9873c79-c000.snappy.parquet"
    val dfSalidaDD: DataFrame = ss.read.parquet(path)

    dfSalidaDD.show(100,false)


    val listaCampos: Seq[String] = Seq("gf_infr_fctr_rwex_adj_amount","gf_original_exposure_amount","gf_mtge_gnt_val_amount","gf_col_rights_gnt_val_amount","gf_after_smes_crfc_rwa_amount",
      "gf_fin_rg_val_amount","gf_customer_pd_per","gf_sg_gnt_eff_sbst_oput_amount","gf_tranche_pd_per","gf_sz_gl_net_bal_nxfer_amount","gf_non_fin_rg_val_amount","gf_guartr_term_residual_number",
      "gf_expected_loss_amount","gf_prvsn_absolute_amount","gf_ownm_eff_sbst_input_amount","gf_ctp_sbst_eff_entry_amount","gf_smes_fctr_rwex_adj_amount","gf_lgd_per")

    val listaCamposHead = listaCampos.head
    val listaCamposTail = listaCampos.tail

    val dfSalidaModificado = listaCamposTail.foldLeft(dfSalidaDD.withColumn(listaCamposHead, col(listaCamposHead)
      .minus(1)))((acumulador, campo) => acumulador.withColumn(campo, col(campo).minus(lit(1))))
    dfSalidaModificado.show(100,false)

    val dfSalida1 = dfSalidaModificado.withColumn("nombre", lit("PEPITO"))
      .withColumn("resta", col("gf_ctn_cred_ccf_per").minus(col("gf_rce_adm_mit_apr_amount"))cast(DecimalType(20,2)))
      .withColumn("gf_ctn_cred_ccf_per", col("gf_ctn_cred_ccf_per").cast(DecimalType(20,2)))

    val campos: Seq[String] = Seq("gf_ctn_cred_ccf_per", "gf_rce_adm_mit_apr_amount", "resta")

    campos.map(_+"_nombreADD"+1)
    val secuenciaCampos: Seq[Column] = campos.map(ele => col(ele).cast(DecimalType(20,2)))


    //dfSalida1.select(col("gf_ctn_cred_ccf_per").cast(DecimalType(20,2)), col("gf_rce_adm_mit_apr_amount"), col("resta")).show(100,false)
    dfSalida1.select(secuenciaCampos:_*).show(100,false)

    //dfSalida1.coalesce(1).write.parquet("C:\\00BBVA\\localHDFS\\data\\master\\kbre\\data\\t_kbre_ex_sub_cred_ma_core_cr\\g_entific_id=UY\\gf_year_month_date_id=202204\\gf_data_stage_type=4\\gf_file_version_desc=1")

    val listaValoresAdmitidos = Seq("VAL1", "VAL2", "VAL3")

    dfSalida1.withColumn("prueba2", when(col("col").isin(listaValoresAdmitidos:_*), lit(1)).otherwise(lit(2)))
  }
}
