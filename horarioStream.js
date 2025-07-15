const AWS = require('aws-sdk');
const { Client } = require('@elastic/elasticsearch');

const REGION = process.env.AWS_REGION || 'us-east-1';
const TABLE_ORG = process.env.TABLE_ORG;
const IP_ES = '34.202.17.116';

const ddb = new AWS.DynamoDB.DocumentClient({ region: REGION });

function calcularMinutos(horaStr) {
  const [h, m] = horaStr.split(':').map(Number);
  return h * 60 + m;
}

exports.handler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;
    console.log(`🟡 Evento: ${eventName}`);

    let horario;
    try {
      if (eventName === 'REMOVE') {
        horario = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);
      } else {
        horario = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
      }
    } catch (e) {
      console.error('❌ Error al deserializar el evento DynamoDB:', e);
      continue;
    }

    console.log('📦 Horario recibido:', JSON.stringify(horario, null, 2));

    const { tenant_id_curso_id, horario_id } = horario;
    if (!tenant_id_curso_id || !horario_id) {
      console.warn(`⚠️ Faltan tenant_id_curso_id o horario_id`);
      continue;
    }

    const [tenant_id, curso_id] = tenant_id_curso_id.split('#');
    const docId = `${tenant_id}#${curso_id}`;

    // Calcular campos auxiliares
    if (horario.inicio_hora) {
      horario.inicio_hora_min = calcularMinutos(horario.inicio_hora);
    }
    if (horario.fin_hora) {
      horario.fin_hora_min = calcularMinutos(horario.fin_hora);
    }

    // 🔍 Obtener puerto desde la tabla de organizaciones
    let puerto;
    try {
      const { Item } = await ddb.get({
        TableName: TABLE_ORG,
        Key: { tenant_id }
      }).promise();

      if (!Item?.puerto) {
        console.warn(`⚠️ Puerto no encontrado para tenant_id ${tenant_id}`);
        continue;
      }

      puerto = Item.puerto;
    } catch (e) {
      console.error(`❌ Error al consultar puerto del tenant ${tenant_id}:`, e);
      continue;
    }

    // Crear cliente de Elasticsearch con el puerto del tenant
    const es = new Client({ node: `http://${IP_ES}:${puerto}` });

    let curso;
    try {
      const { _source } = await es.get({
        index: 'cursos',
        id: docId
      });
      curso = _source;
      curso.horarios = curso.horarios || [];
    } catch (err) {
      if (err.meta?.statusCode === 404) {
        console.warn(`⚠️ Curso no encontrado en Elasticsearch: ${docId}`);
        continue;
      }
      console.error(`❌ Error al obtener curso ${docId}:`, err);
      continue;
    }

    console.log(`📘 Documento de curso encontrado:`, JSON.stringify(curso, null, 2));

    try {
      if (eventName === 'INSERT') {
        const yaExiste = curso.horarios.some(h => h.horario_id === horario_id);
        if (!yaExiste) {
          curso.horarios.push(horario);
          console.log(`✅ Horario INSERTADO: ${horario_id}`);
        } else {
          console.log(`🔁 Horario ya existía, no se volvió a insertar: ${horario_id}`);
        }

      } else if (eventName === 'MODIFY') {
        curso.horarios = curso.horarios.map(h =>
          h.horario_id === horario_id ? horario : h
        );
        console.log(`🔧 Horario MODIFICADO: ${horario_id}`);

      } else if (eventName === 'REMOVE') {
        curso.horarios = curso.horarios.filter(h => h.horario_id !== horario_id);
        console.log(`🗑️ Horario ELIMINADO: ${horario_id}`);
      } else {
        console.warn(`⚠️ Evento no manejado: ${eventName}`);
        continue;
      }

      await es.index({
        index: 'cursos',
        id: docId,
        document: curso
      });

      console.log(`📤 Curso actualizado en Elasticsearch con ID: ${docId} (puerto: ${puerto})`);

    } catch (err) {
      console.error(`❌ Error al procesar horario (${eventName}):`, err);
    }
  }

  return { statusCode: 200 };
};
