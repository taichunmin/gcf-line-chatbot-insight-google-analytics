const _ = require('lodash')
const axios = require('axios')
const Line = require('@line/bot-sdk').Client
const moment = require('moment')
const Papa = require('papaparse')
const Qs = require('qs')

const BATCH_LIMIT = 20

exports.getCsv = async url => {
  const csv = _.trim(_.get(await axios.get(url), 'data'))
  return _.get(Papa.parse(csv, {
    encoding: 'utf8',
    header: true,
  }), 'data', [])
}

exports.getenv = (key, defaultval) => {
  return _.get(process, ['env', key], defaultval)
}

exports.getBots = async csv => {
  try {
    const bots = await exports.getCsv(csv)
    return bots
  } catch (err) {
    console.error(err)
    return []
  }
}

exports.getBotInsightMessageDelivery = async ({ date, hits, line }) => {
  try {
    const data = await line.getNumberOfMessageDeliveries(date)
    if (_.get(data, 'status') !== 'ready') return
    _.each([
      'apiBroadcast',
      'apiMulticast',
      'apiNarrowcast',
      'apiPush',
      'apiReply',
      'autoResponse',
      'broadcast',
      'chat',
      'targeting',
      'welcomeResponse',
    ], k => {
      if (!_.hasIn(data, k)) return
      hits.push({ ea: `messageDelivery-${k}`, el: date, ev: _.get(data, k) })
    })
  } catch (err) {
    console.error(err)
  }
}

exports.getBotInsightFollowers = async ({ date, hits, line }) => {
  try {
    const data = await line.getNumberOfFollowers(date)
    if (_.get(data, 'status') !== 'ready') return
    _.each([
      'followers',
      'targetedReaches',
      'blocks',
    ], k => {
      if (!_.hasIn(data, k)) return
      hits.push({ ea: `followers-${k}`, el: date, ev: _.get(data, k) })
    })
  } catch (err) {
    console.error(err)
  }
}

exports.getBotInsightDemographic = async ({ date, hits, line }) => {
  try {
    const data = await line.getFriendDemographics()
    if (_.get(data, 'available') !== true) return
    _.each(data.genders, gender => {
      hits.push({ ea: `demographic-genders-${gender.gender}`, el: date, ev: _.round(gender.percentage * 10) })
    })
    _.each(data.ages, age => {
      hits.push({ ea: `demographic-ages-${age.age}`, el: date, ev: _.round(age.percentage * 10) })
    })
    _.each(data.areas, area => {
      hits.push({ ea: `demographic-areas-${area.area}`, el: date, ev: _.round(area.percentage * 10) })
    })
    _.each(data.appTypes, appType => {
      hits.push({ ea: `demographic-appTypes-${appType.appType}`, el: date, ev: _.round(appType.percentage * 10) })
    })
    _.each(data.subscriptionPeriods, subscriptionPeriod => {
      hits.push({ ea: `demographic-subscriptionPeriods-${subscriptionPeriod.subscriptionPeriod}`, el: date, ev: _.round(subscriptionPeriod.percentage * 10) })
    })
  } catch (err) {
    console.error(err)
  }
}

exports.randomUuid = (() => {
  const r4 = () => _.padStart(_.random(65535).toString(16), 4, '0')
  return () => `${r4()}${r4()}-${r4()}-${r4()}-${r4()}-${r4()}${r4()}${r4()}`
})()

exports.httpBuildQuery = obj => Qs.stringify(obj, { arrayFormat: 'brackets' })

exports.sendInsightGa = async ({ bot, hits }) => {
  const hitDefault = {
    aip: 1,
    an: 'LINE Insight',
    av: '1.0.0',
    ds: 'app',
    tid: bot.tracking_id,
    v: 1,
    cid: exports.randomUuid(),
  }
  const payloads = [
    { ...hitDefault, qt: 3e4, t: 'screenview', cd: bot.name },
    ..._.map(hits, hit => ({ ...hitDefault, t: 'event', ec: bot.name, ...hit })),
  ]
  const chunks = _.chunk(payloads, BATCH_LIMIT)
  await Promise.all(_.map(chunks, async chunk => {
    try {
      // console.log('sendInsightGa chunk =', JSON.stringify(chunk))
      const body = _.join(_.map(chunk, exports.httpBuildQuery), '\r\n')
      await axios.post('https://www.google-analytics.com/batch', body)
    } catch (err) {
      console.error(err)
      console.log('sendInsightGa chunk =', JSON.stringify(chunk))
    }
  }))
}

exports.getBotInsight = async ({ hits, line }) => {
  const dates = _.map(_.range(3), d => moment('0+0900', 'HHZ').utcOffset(9).add(d - 3, 'd').format('YYYYMMDD'))
  await Promise.all([
    ..._.map(dates, date => exports.getBotInsightMessageDelivery({ date, hits, line })),
    ..._.map(dates, date => exports.getBotInsightFollowers({ date, hits, line })),
    exports.getBotInsightDemographic({ date: moment().utcOffset(9).format('YYYYMMDDHH'), hits, line }),
  ])
}

exports.main = async () => {
  const bots = await exports.getBots(exports.getenv('BOTS_CSV'))

  for (const bot of bots) {
    try {
      const line = new Line({ channelAccessToken: bot.access_token })
      const hits = []
      await exports.getBotInsight({ bot, hits, line })
      await exports.sendInsightGa({ bot, hits })
    } catch (err) {
      console.error(err)
    }
  }
}
