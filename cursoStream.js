const AWS = require('aws-sdk');
const { Client } = require('@elastic/elasticsearch');

const TABLE_ORG = process.env.TABLE_ORG;
const IP_ES = '34.233.20.17'; // IP base de la VM

const dynamodb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;

    try {
      if (eventName === 'INSERT' || eventName === 'MODIFY') {
        const newImage = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
        const tenant_id = newImage.tenant_id;
        const curso_id = newImage.curso_id;

        // Obtener el puerto del tenant
        const orgData = await dynamodb.get({
          TableName: TABLE_ORG,
          Key: { tenant_id }
        }).promise();

        const puerto = orgData.Item?.puerto;
        if (!puerto) {
          console.error(`❌ No se encontró el puerto para tenant_id: ${tenant_id}`);
          continue;
        }

        // Crear cliente Elasticsearch dinámico
        const es = new Client({ node: `http://${IP_ES}:${puerto}` });
        const esId = `${tenant_id}#${curso_id}`;

        // Armar documento
        const doc = {
          curso_id: curso_id,
          tenant_id: tenant_id,
          nombre: newImage.nombre,
          descripcion: newImage.descripcion,
          inicio: newImage.inicio,
          fin: newImage.fin,
          precio: newImage.precio,
          horarios: newImage.horarios || []
        };

        // Indexar
        await es.index({
          index: 'cursos',
          id: esId,
          document: doc
        });

        console.log(`✅ Curso ${esId} sincronizado a Elasticsearch (${puerto})`);

      } else if (eventName === 'REMOVE') {
        const oldKeys = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.Keys);
        const tenant_id = oldKeys.tenant_id;
        const curso_id = oldKeys.curso_id;

        // Obtener el puerto del tenant
        const orgData = await dynamodb.get({
          TableName: TABLE_ORG,
          Key: { tenant_id }
        }).promise();

        const puerto = orgData.Item?.puerto;
        if (!puerto) {
          console.error(`❌ No se encontró el puerto para tenant_id: ${tenant_id}`);
          continue;
        }

        const es = new Client({ node: `http://${IP_ES}:${puerto}` });
        const esId = `${tenant_id}#${curso_id}`;

        // Eliminar documento
        await es.delete({
          index: 'cursos',
          id: esId
        }).catch(err => {
          if (err.meta?.statusCode !== 404) throw err;
          console.warn(`⚠️ Documento no encontrado para eliminar: ${esId}`);
        });

        console.log(`🗑️ Curso ${esId} eliminado de Elasticsearch (${puerto})`);
      }

    } catch (err) {
      console.error(`❌ Error procesando evento ${eventName}:`, err);
    }
  }

  return { statusCode: 200 };
};
