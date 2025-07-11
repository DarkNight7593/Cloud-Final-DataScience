const AWS = require('aws-sdk');
const { Client } = require('@elastic/elasticsearch');

const es = new Client({ node: 'http://34.233.20.17:9201' }); // IP de Elasticsearch
const INDEX_CURSO = 'cursos';

function calcularMinutos(horaStr) {
  const [h, m] = horaStr.split(':').map(Number);
  return h * 60 + m;
}

exports.handler = async (event) => {
  for (const record of event.Records) {
    const eventName = record.eventName;

    let horario;
    if (eventName === 'REMOVE') {
      horario = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.OldImage);
    } else {
      horario = AWS.DynamoDB.Converter.unmarshall(record.dynamodb.NewImage);
    }

    const { tenant_id_curso_id, horario_id } = horario;

    // Descomponer tenant_id y curso_id desde la clave compuesta
    const [tenant_id, curso_id] = tenant_id_curso_id.split('#');
    const docId = `${tenant_id}#${curso_id}`;

    // Calcular campos auxiliares
    if (horario.inicio_hora) {
      horario.inicio_hora_min = calcularMinutos(horario.inicio_hora);
    }
    if (horario.fin_hora) {
      horario.fin_hora_min = calcularMinutos(horario.fin_hora);
    }

    try {
      const { body: existing } = await es.get({
        index: INDEX_CURSO,
        id: docId
      });

      const curso = existing._source;
      curso.horarios = curso.horarios || [];

      if (eventName === 'INSERT') {
        const yaExiste = curso.horarios.some(h => h.horario_id === horario_id);
        if (!yaExiste) {
          curso.horarios.push(horario);
        }

      } else if (eventName === 'MODIFY') {
        curso.horarios = curso.horarios.map(h =>
          h.horario_id === horario_id ? horario : h
        );

      } else if (eventName === 'REMOVE') {
        curso.horarios = curso.horarios.filter(h => h.horario_id !== horario_id);
      }

      // Reindexar el curso con los nuevos horarios
      await es.index({
        index: INDEX_CURSO,
        id: docId,
        document: curso
      });

    } catch (err) {
      if (err.meta?.statusCode === 404) {
        console.warn(`Curso ${docId} no encontrado en Elasticsearch`);
        continue;
      }
      console.error(`Error al sincronizar horario (${eventName}):`, err);
      throw err;
    }
  }

  return { statusCode: 200 };
};
