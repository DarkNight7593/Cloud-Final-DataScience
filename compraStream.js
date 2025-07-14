const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const BUCKET = process.env.BUCKET_COMPRAS;

exports.handler = async ({ Records }) => {
  for (const record of Records) {
    const { eventName, dynamodb: ddb } = record;
    const image = eventName === 'REMOVE' ? ddb.OldImage : ddb.NewImage;
    const item = AWS.DynamoDB.Converter.unmarshall(image);
    const { tenant_id_dni_estado, curso_id } = item;

    if (!tenant_id_dni_estado || !curso_id) {
      console.warn(`⚠️ Faltan llaves clave para evento ${eventName}`);
      continue;
    }

    const [tenant_id] = tenant_id_dni_estado.split('#');
    const fileName = `${tenant_id_dni_estado}#${curso_id}.json`;
    const objectKey = `${tenant_id}/${fileName}`;

    if (eventName === 'REMOVE') {
      try {
        await s3.deleteObject({ Bucket: BUCKET, Key: objectKey }).promise();
        console.log(`🗑️ Eliminado JSON: ${objectKey}`);
      } catch (err) {
        console.error(`❌ Error eliminando JSON: ${objectKey}`, err);
      }
      continue;
    }

    const {
      curso_nombre,
      alumno_dni,
      alumno_nombre,
      instructor_dni,
      instructor_nombre,
      estado,
      horario_id,
      dias,
      inicio_hora,
      fin_hora,
      precio,
      fecha_inicio,
      fecha_fin
    } = item;

    const obj = {
      curso_id,
      curso_nombre,
      alumno_dni,
      alumno_nombre,
      instructor_dni,
      instructor_nombre,
      estado,
      horario_id,
      dias: dias ?? [],
      inicio_hora: normalizarHora(inicio_hora),
      fin_hora: normalizarHora(fin_hora),
      precio,
      fecha_inicio: normalizarFecha(fecha_inicio),
      fecha_fin: normalizarFecha(fecha_fin)
    };

    try {
      await s3.putObject({
        Bucket: BUCKET,
        Key: objectKey,
        Body: JSON.stringify(obj),
        ContentType: 'application/json'
      }).promise();
      console.log(`✅ JSON generado: ${objectKey}`);
    } catch (err) {
      console.error(`❌ Error subiendo JSON: ${objectKey}`, err);
    }
  }

  return { statusCode: 200, body: 'OK' };
};

function normalizarHora(h) {
  if (typeof h !== 'string') return null;
  const m = h.match(/^\d{2}:\d{2}/);
  return m ? `${m[0]}:00` : null;
}

function normalizarFecha(f) {
  if (typeof f !== 'string') return null;
  const match = f.match(/^\d{4}-\d{2}-\d{2}/); // formato YYYY-MM-DD
  return match ? match[0] : null;
}
