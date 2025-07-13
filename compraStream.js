const AWS = require('aws-sdk');
const s3 = new AWS.S3();

const BUCKET = process.env.BUCKET_COMPRAS;

exports.handler = async ({ Records }) => {
  for (const record of Records) {
    const { eventName, dynamodb: ddb } = record;

    const newItem = ddb.NewImage
      ? AWS.DynamoDB.Converter.unmarshall(ddb.NewImage)
      : null;

    const oldItem = ddb.OldImage
      ? AWS.DynamoDB.Converter.unmarshall(ddb.OldImage)
      : null;

    const item = newItem || oldItem;
    const tenant_id = item?.tenant_id;
    const pk = item?.tenant_id_dni_estado;
    const curso_id = item?.curso_id;

    if (!tenant_id || !pk || !curso_id) {
      console.warn('Datos faltantes');
      continue;
    }

    const fileName = `${pk}#${curso_id}.csv`;
    const objectKey = `${tenant_id}/${fileName}`;

    if (eventName === 'REMOVE') {
      await s3.deleteObject({ Bucket: BUCKET, Key: objectKey }).promise();
      console.log(`Eliminado: ${objectKey}`);
      continue;
    }

    const fields = [
      item.curso_id,
      item.curso_nombre,
      item.alumno_dni,
      item.alumno_nombre,
      item.instructor_dni,
      item.instructor_nombre,
      item.estado,
      item.horario_id,
      item.dias,
      normalizarFecha(item.inicio),
      normalizarFecha(item.fin),
      normalizarHora(item.inicio_hora),
      normalizarHora(item.fin_hora),
      item.precio
    ];

    const line = fields.map(v => `"${String(v ?? '').replace(/"/g, '""')}"`).join(',');

    await s3.putObject({
      Bucket: BUCKET,
      Key: objectKey,
      Body: line + '\n',
      ContentType: 'text/csv'
    }).promise();

    console.log(`Actualizado: ${objectKey}`);
  }

  return { statusCode: 200, body: 'OK' };
};

function normalizarFecha(f) {
  // Acepta strings tipo "2025-07-13" o Date y convierte siempre a yyyy-MM-dd
  try {
    const d = typeof f === 'string' ? new Date(f) : f instanceof Date ? f : null;
    if (!d || isNaN(d)) return '';
    return d.toISOString().split('T')[0]; // yyyy-MM-dd
  } catch {
    return '';
  }
}

function normalizarHora(h) {
  // Acepta "HH:mm" o "HH:mm:ss" y retorna "HH:mm"
  if (typeof h !== 'string') return '';
  const hhmm = h.match(/^\d{2}:\d{2}/);
  return hhmm ? hhmm[0] : '';
}
