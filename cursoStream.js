const AWS = require('aws-sdk');
const { Client } = require('@elastic/elasticsearch');

const TABLE_ORG = process.env.TABLE_ORG;
const IP_ES = '34.233.20.17';

const ddb = new AWS.DynamoDB.DocumentClient();

exports.handler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;

    try {
      const isRemove = eventName === 'REMOVE';
      const data = AWS.DynamoDB.Converter.unmarshall(
        isRemove ? record.dynamodb.OldImage : record.dynamodb.NewImage
      );

      const { tenant_id, curso_id } = data;
      if (!tenant_id || !curso_id) {
        console.warn(`⚠️ Faltan tenant_id o curso_id`);
        continue;
      }

      const docId = `${tenant_id}#${curso_id}`;

      // Obtener el puerto desde la tabla de organizaciones
      const { Item } = await ddb.get({
        TableName: TABLE_ORG,
        Key: { tenant_id }
      }).promise();

      const puerto = Item?.puerto;
      if (!puerto) {
        console.error(`❌ No se encontró el puerto para tenant_id: ${tenant_id}`);
        continue;
      }

      const es = new Client({ node: `http://${IP_ES}:${puerto}` });

      if (isRemove) {
        // Eliminar documento de Elasticsearch
        await es.delete({
          index: 'cursos',
          id: docId
        }).catch(err => {
          if (err.meta?.statusCode !== 404) throw err;
          console.warn(`⚠️ Documento no encontrado para eliminar: ${docId}`);
        });

        console.log(`🗑️ Curso ${docId} eliminado de Elasticsearch (${puerto})`);
        continue;
      }

      if (eventName === 'INSERT') {
        // Insert inicial: no tiene horarios todavía
        const doc = {
          curso_id,
          nombre: data.nombre,
          descripcion: data.descripcion,
          inicio: data.inicio,
          fin: data.fin,
          precio: data.precio,
          horarios: []  // aún no hay
        };

        await es.index({
          index: 'cursos',
          id: docId,
          document: doc
        });

        console.log(`✅ Curso INSERTADO sin horarios en Elasticsearch: ${docId}`);
        continue;
      }

      if (eventName === 'MODIFY') {
        // Obtener documento actual
        let docActual;
        try {
          const { _source } = await es.get({
            index: 'cursos',
            id: docId
          });
          docActual = _source;
        } catch (err) {
          if (err.meta?.statusCode === 404) {
            console.warn(`⚠️ Documento no encontrado (MODIFY sin INSERT previo): ${docId}`);
            continue;
          }
          throw err;
        }

        const horariosAnteriores = docActual.horarios || [];

        const docActualizado = {
          curso_id,
          nombre: data.nombre,
          descripcion: data.descripcion,
          inicio: data.inicio,
          fin: data.fin,
          precio: data.precio,
          horarios: horariosAnteriores
        };

        await es.index({
          index: 'cursos',
          id: docId,
          document: docActualizado
        });

        console.log(`🔄 Curso MODIFICADO conservando horarios en Elasticsearch: ${docId}`);
      }

    } catch (err) {
      console.error(`❌ Error procesando evento ${eventName}:`, err);
    }
  }

  return { statusCode: 200 };
};
