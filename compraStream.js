const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const BUCKET = process.env.BUCKET_COMPRAS;

exports.handler = async ({ Records }) => {
  for (const record of Records) {
    const { eventName, dynamodb: ddb } = record;

    if (eventName === 'REMOVE') {
      const oldItem = AWS.DynamoDB.Converter.unmarshall(ddb.OldImage);
      const { tenant_id, tenant_id_dni_estado, curso_id } = oldItem;

      if (!tenant_id || !tenant_id_dni_estado || !curso_id) {
        console.warn('Datos faltantes para eliminar');
        continue;
      }

      const fileName = `${tenant_id_dni_estado}#${curso_id}.csv`;
      const objectKey = `${tenant_id}/${fileName}`;

      await s3.deleteObject({ Bucket: BUCKET, Key: objectKey }).promise();
      console.log(`🗑️ Eliminado: ${objectKey}`);
      continue;
    }

    if (eventName === 'INSERT' || eventName === 'MODIFY') {
      const newItem = AWS.DynamoDB.Converter.unmarshall(ddb.NewImage);
      const {
        tenant_id,
        tenant_id_dni_estado: pk,
        curso_id,
        curso_nombre,
        alumno_dni,
        alumno_nombre,
        instructor_dni,
        instructor_nombre,
        estado,
        horario_id,
        dias,
        inicio,
        fin,
        inicio_hora,
        fin_hora,
        precio
      } = newItem;

      if (!tenant_id || !pk || !curso_id) {
        console.warn('Datos faltantes para insertar o modificar');
        continue;
      }

      const fileName = `${pk}#${curso_id}.csv`;
      const objectKey = `${tenant_id}/${fileName}`;

      const fields = [
        curso_id,
        curso_nombre,
        alumno_dni,
        alumno_nombre,
        instructor_dni,
        instructor_nombre,
        estado,
        horario_id,
        dias,
        normalizarFecha(inicio),
        normalizarFecha(fin),
        normalizarHora(inicio_hora),
        normalizarHora(fin_hora),
        precio
      ];

      const line = fields.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',');

      await s3.putObject({
        Bucket: BUCKET,
        Key: objectKey,
        Body: line + '\n',
        ContentType: 'text/csv'
      }).promise();

      console.log(`✅ Actualizado: ${objectKey}`);
    }
  }

  return { statusCode: 200, body: 'OK' };
};

// Formato compatible con Athena y Glue
function normalizarFecha(f) {
  try {
    const d = typeof f === 'string' ? new Date(f) : f instanceof Date ? f : null;
    if (!d || isNaN(d)) return '';
    return d.toISOString().split('T')[0]; // yyyy-MM-dd
  } catch {
    return '';
  }
}

function normalizarHora(h) {
  if (typeof h !== 'string') return '';
  const hhmm = h.match(/^\d{2}:\d{2}/);
  return hhmm ? hhmm[0] : '';
}
