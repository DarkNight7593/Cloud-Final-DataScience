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
      console.warn(`⚠️ Datos faltantes para evento ${eventName}`);
      continue;
    }

    const [tenant_id] = tenant_id_dni_estado.split('#');
    const fileName = `${tenant_id_dni_estado}#${curso_id}.csv`;
    const objectKey = `${tenant_id}/${fileName}`;

    if (eventName === 'REMOVE') {
      try {
        await s3.deleteObject({
          Bucket: BUCKET,
          Key: objectKey
        }).promise();
        console.log(`🗑️ Eliminado: ${objectKey}`);
      } catch (e) {
        console.error(`❌ Error eliminando archivo: ${objectKey}`, e);
      }
      continue;
    }

    // INSERT o MODIFY
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
      precio
    } = item;

    const fields = [
      curso_id,
      curso_nombre,
      alumno_dni,
      alumno_nombre,
      instructor_dni,
      instructor_nombre,
      estado,
      horario_id,
      JSON.stringify(dias ?? []), // Serializar como array
      normalizarHoraComoTime(inicio_hora),
      normalizarHoraComoTime(fin_hora),
      precio
    ];

    const line = fields.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',');

    try {
      await s3.putObject({
        Bucket: BUCKET,
        Key: objectKey,
        Body: line + '\n',
        ContentType: 'text/csv'
      }).promise();
      console.log(`✅ Actualizado: ${objectKey}`);
    } catch (e) {
      console.error(`❌ Error subiendo archivo: ${objectKey}`, e);
    }
  }

  return { statusCode: 200, body: 'OK' };
};

// Formato Athena TIME
function normalizarHoraComoTime(h) {
  if (typeof h !== 'string') return '';
  const hhmm = h.match(/^\d{2}:\d{2}/);
  return hhmm ? `${hhmm[0]}:00` : '';
}
